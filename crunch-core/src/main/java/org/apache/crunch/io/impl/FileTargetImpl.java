/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.crunch.io.impl;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mr.plan.PlanningParameters;
import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.crunch.io.CrunchOutputs;
import org.apache.crunch.io.FileNamingScheme;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.OutputHandler;
import org.apache.crunch.io.PathTarget;
import org.apache.crunch.io.SourceTargetHelper;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileTargetImpl implements PathTarget {

  private static final Logger LOG = LoggerFactory.getLogger(FileTargetImpl.class);
  
  protected final Path path;
  private final FormatBundle<? extends FileOutputFormat> formatBundle;
  private final FileNamingScheme fileNamingScheme;

  public FileTargetImpl(Path path, Class<? extends FileOutputFormat> outputFormatClass,
                        FileNamingScheme fileNamingScheme) {
    this(path, outputFormatClass, fileNamingScheme, ImmutableMap.<String, String>of());
  }

  public FileTargetImpl(Path path, Class<? extends FileOutputFormat> outputFormatClass,
      FileNamingScheme fileNamingScheme, Map<String, String> extraConf) {
    this.path = path;
    this.formatBundle = FormatBundle.forOutput(outputFormatClass);
    this.fileNamingScheme = fileNamingScheme;
    if (extraConf != null && !extraConf.isEmpty()) {
      for (Map.Entry<String, String> e : extraConf.entrySet()) {
        formatBundle.set(e.getKey(), e.getValue());
      }
    }
  }

  @Override
  public Target outputConf(String key, String value) {
    formatBundle.set(key, value);
    return this;
  }

  @Override
  public void configureForMapReduce(Job job, PType<?> ptype, Path outputPath, String name) {
    Converter converter = getConverter(ptype);
    Class keyClass = converter.getKeyClass();
    Class valueClass = converter.getValueClass();
    configureForMapReduce(job, keyClass, valueClass, formatBundle, outputPath, name);
  }

  @Deprecated
  protected void configureForMapReduce(Job job, Class keyClass, Class valueClass,
      Class outputFormatClass, Path outputPath, String name) {
    configureForMapReduce(job, keyClass, valueClass, FormatBundle.forOutput(outputFormatClass), outputPath, name);
  }

  protected void configureForMapReduce(Job job, Class keyClass, Class valueClass,
      FormatBundle formatBundle, Path outputPath, String name) {
    try {
      FileOutputFormat.setOutputPath(job, outputPath);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    if (name == null) {
      job.setOutputFormatClass(formatBundle.getFormatClass());
      formatBundle.configure(job.getConfiguration());
      job.setOutputKeyClass(keyClass);
      job.setOutputValueClass(valueClass);
    } else {
      CrunchOutputs.addNamedOutput(job, name, formatBundle, keyClass, valueClass);
    }
  }

  @Override
  public boolean accept(OutputHandler handler, PType<?> ptype) {
    handler.configure(this, ptype);
    return true;
  }

  @Override
  public Converter<?, ?, ?, ?> getConverter(PType<?> ptype) {
    return ptype.getConverter();
  }

  private class WorkingPathFileMover implements Callable<Boolean> {
    private Configuration conf;
    private Path src;
    private Path dst;
    private FileSystem srcFs;
    private FileSystem dstFs;
    private boolean sameFs;


    public WorkingPathFileMover(Configuration conf, Path src, Path dst,
                                FileSystem srcFs, FileSystem dstFs, boolean sameFs) {
      this.conf = conf;
      this.src = src;
      this.dst = dst;
      this.srcFs = srcFs;
      this.dstFs = dstFs;
      this.sameFs = sameFs;
    }

    @Override
    public Boolean call() throws IOException {
      if (sameFs) {
        return srcFs.rename(src, dst);
      } else {
        return FileUtil.copy(srcFs, src, dstFs, dst, true, true, conf);
      }
    }
  }

  @Override
  public void handleOutputs(Configuration conf, Path workingPath, int index) throws IOException {
    FileSystem srcFs = workingPath.getFileSystem(conf);
    Path src = getSourcePattern(workingPath, index);
    Path[] srcs = FileUtil.stat2Paths(srcFs.globStatus(src), src);
    FileSystem dstFs = path.getFileSystem(conf);
    if (!dstFs.exists(path)) {
      dstFs.mkdirs(path);
    }
    boolean sameFs = isCompatible(srcFs, path);
    List<ListenableFuture<Boolean>> renameFutures = Lists.newArrayList();
    ListeningExecutorService executorService =
        MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(
                conf.getInt(RuntimeParameters.FILE_TARGET_MAX_THREADS, 1)));
    for (Path s : srcs) {
      Path d = getDestFile(conf, s, path, s.getName().contains("-m-"));
      renameFutures.add(
          executorService.submit(
              new WorkingPathFileMover(conf, s, d, srcFs, dstFs, sameFs)));
    }
    LOG.debug("Renaming " + renameFutures.size() + " files.");

    ListenableFuture<List<Boolean>> future =
        Futures.successfulAsList(renameFutures);
    List<Boolean> renameResults = null;
    try {
      renameResults = future.get();
    } catch (InterruptedException | ExecutionException e) {
      Throwables.propagate(e);
    } finally {
      executorService.shutdownNow();
    }
    if (renameResults != null && !renameResults.contains(false)) {
      dstFs.create(getSuccessIndicator(), true).close();
      LOG.debug("Renamed " + renameFutures.size() + " files.");
    }
  }
  
  protected Path getSuccessIndicator() {
    return new Path(path, "_SUCCESS");
  }
  
  protected Path getSourcePattern(Path workingPath, int index) {
    if (index < 0) {
      return new Path(workingPath, "part-*");
    } else {
      return new Path(workingPath, PlanningParameters.MULTI_OUTPUT_PREFIX + index + "-*");
    }
  }
  
  @Override
  public Path getPath() {
    return path;
  }
  
  protected static boolean isCompatible(FileSystem fs, Path path) {
    try {
      fs.makeQualified(path);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  protected Path getDestFile(Configuration conf, Path src, Path dir, boolean mapOnlyJob)
      throws IOException {
    String outputFilename = null;
    String sourceFilename = src.getName();
    if (mapOnlyJob) {
      outputFilename = getFileNamingScheme().getMapOutputName(conf, dir);
    } else {
      outputFilename = getFileNamingScheme().getReduceOutputName(conf, dir, extractPartitionNumber(sourceFilename));
    }
    if (sourceFilename.contains(".")) {
      outputFilename += sourceFilename.substring(sourceFilename.indexOf("."));
    }
    return new Path(dir, outputFilename);
  }
  
  /**
   * Extract the partition number from a raw reducer output filename.
   *
   * @param reduceOutputFileName The raw reducer output file name
   * @return The partition number encoded in the filename
   */
  public static int extractPartitionNumber(String reduceOutputFileName) {
    Matcher matcher = Pattern.compile(".*-r-(\\d{5})").matcher(reduceOutputFileName);
    if (matcher.find()) {
      return Integer.parseInt(matcher.group(1), 10);
    } else {
      throw new IllegalArgumentException("Reducer output name '" + reduceOutputFileName + "' cannot be parsed");
    }
  }
  
  @Override
  public FileNamingScheme getFileNamingScheme() {
    return fileNamingScheme;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !getClass().equals(other.getClass())) {
      return false;
    }
    FileTargetImpl o = (FileTargetImpl) other;
    return path.equals(o.path);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(path).toHashCode();
  }

  @Override
  public String toString() {
    return new StringBuilder()
        .append(formatBundle.getFormatClass().getSimpleName())
        .append("(")
        .append(path)
        .append(")")
        .toString();
  }

  @Override
  public <T> SourceTarget<T> asSourceTarget(PType<T> ptype) {
    // By default, assume that we cannot do this.
    return null;
  }

  @Override
  public boolean handleExisting(WriteMode strategy, long lastModForSource, Configuration conf) {
    FileSystem fs = null;
    try {
      fs = path.getFileSystem(conf);
    } catch (IOException e) {
      LOG.error("Could not retrieve FileSystem object to check for existing path", e);
      throw new CrunchRuntimeException(e);
    }
    
    boolean exists = false;
    boolean successful = false;
    long lastModForTarget = -1;
    try {
      exists = fs.exists(path);
      if (exists) {
        successful = fs.exists(getSuccessIndicator());
        lastModForTarget = SourceTargetHelper.getLastModifiedAt(fs, path);
      }
    } catch (IOException e) {
      LOG.error("Exception checking existence of path: {}", path, e);
      throw new CrunchRuntimeException(e);
    }
    
    if (exists) {
      switch (strategy) {
      case DEFAULT:
        LOG.error("Path {} already exists!", path);
        throw new CrunchRuntimeException("Path already exists: " + path);
      case OVERWRITE:
        LOG.info("Removing data at existing path: {}", path);
        try {
          fs.delete(path, true);
        } catch (IOException e) {
          LOG.error("Exception thrown removing data at path: {}", path, e);
        }
        break;
      case APPEND:
        LOG.info("Adding output files to existing path: {}", path);
        break;
      case CHECKPOINT:
        if (successful && lastModForTarget > lastModForSource) {
          LOG.info("Re-starting pipeline from checkpoint path: {}", path);
          break;
        } else {
          if (!successful) {
            LOG.info("_SUCCESS file not found, Removing data at existing checkpoint path: {}", path);
          } else {
            LOG.info("Source data has recent updates. Removing data at existing checkpoint path: {}", path);
          }
          try {
            fs.delete(path, true);
          } catch (IOException e) {
            LOG.error("Exception thrown removing data at checkpoint path: {}", path, e);
          }
          return false;
        }
      default:
        throw new CrunchRuntimeException("Unknown WriteMode:  " + strategy);
      }
    } else {
      LOG.info("Will write output files to new path: {}", path);
    }
    return exists;
  }

}
