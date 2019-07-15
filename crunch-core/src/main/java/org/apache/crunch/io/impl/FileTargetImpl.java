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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
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
import org.apache.crunch.util.CrunchRenameCopyListing;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.tools.CopyListing;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.DistCpOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileTargetImpl implements PathTarget {

  private static final Logger LOG = LoggerFactory.getLogger(FileTargetImpl.class);
  
  protected Path path;
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
  public Target fileSystem(FileSystem fileSystem) {
    if (formatBundle.getFileSystem() != null) {
      throw new IllegalStateException("Filesystem already set. Change is not supported.");
    }

    if (fileSystem != null) {
      path = fileSystem.makeQualified(path);

      formatBundle.setFileSystem(fileSystem);
    }
    return this;
  }

  @Override
  public FileSystem getFileSystem() {
    return formatBundle.getFileSystem();
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
    Configuration dstFsConf = getEffectiveBundleConfig(conf);
    FileSystem dstFs = path.getFileSystem(dstFsConf);
    if (!dstFs.exists(path)) {
      dstFs.mkdirs(path);
    }
    Path srcPattern = getSourcePattern(workingPath, index);
    boolean sameFs = isCompatible(srcFs, path);
    boolean useDistributedCopy = conf.getBoolean(RuntimeParameters.FILE_TARGET_USE_DISTCP, true);
    int maxDistributedCopyTasks = conf.getInt(RuntimeParameters.FILE_TARGET_MAX_DISTCP_TASKS, 100);
    int maxDistributedCopyTaskBandwidthMB = conf.getInt(RuntimeParameters.FILE_TARGET_MAX_DISTCP_TASK_BANDWIDTH_MB,
        DistCpConstants.DEFAULT_BANDWIDTH_MB);
    int maxThreads = conf.getInt(RuntimeParameters.FILE_TARGET_MAX_THREADS, 1);

    if (!sameFs) {
      if (useDistributedCopy) {
        LOG.info("Source and destination are in different file systems, performing distributed copy from {} to {}", srcPattern,
            path);
        handleOutputsDistributedCopy(conf, srcPattern, srcFs, dstFs, maxDistributedCopyTasks,
            maxDistributedCopyTaskBandwidthMB);
      } else {
        LOG.info("Source and destination are in different file systems, performing asynch copies from {} to {}", srcPattern, path);
        handleOutputsAsynchronously(conf, srcPattern, srcFs, dstFs, sameFs, maxThreads);
      }
    } else {
      LOG.info("Source and destination are in the same file system, performing asynch renames from {} to {}", srcPattern, path);
      handleOutputsAsynchronously(conf, srcPattern, srcFs, dstFs, sameFs, maxThreads);
    }
  }

  private void handleOutputsAsynchronously(Configuration conf, Path srcPattern, FileSystem srcFs, FileSystem dstFs,
          boolean sameFs, int maxThreads) throws IOException {
    Configuration dstFsConf = getEffectiveBundleConfig(conf);
    Path[] srcs = FileUtil.stat2Paths(srcFs.globStatus(srcPattern), srcPattern);
    List<ListenableFuture<Boolean>> renameFutures = Lists.newArrayList();
    ListeningExecutorService executorService =
        MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(
                maxThreads));
    for (Path s : srcs) {
      Path d = getDestFile(dstFsConf, s, path, s.getName().contains("-m-"));
      renameFutures.add(
          executorService.submit(
              new WorkingPathFileMover(conf, s, d, srcFs, dstFs, sameFs)));
    }
    if (sameFs) {
      LOG.info("Renaming {} files using at most {} threads.", renameFutures.size(), maxThreads);
    } else {
      LOG.info("Copying {} files using at most {} threads.", renameFutures.size(), maxThreads);
    }
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
      if (sameFs) {
        LOG.info("Renamed {} files.", renameFutures.size());
      } else {
        LOG.info("Copied {} files.", renameFutures.size());
      }
      dstFs.create(getSuccessIndicator(), true).close();
      LOG.info("Created success indicator file");
    }
  }

  private void handleOutputsDistributedCopy(Configuration conf, Path srcPattern, FileSystem srcFs, FileSystem dstFs,
          int maxTasks, int maxBandwidthMB) throws IOException {
    Configuration dstFsConf = getEffectiveBundleConfig(conf);
    Path[] srcs = FileUtil.stat2Paths(srcFs.globStatus(srcPattern), srcPattern);
    if (srcs.length > 0) {
      try {
        DistCp distCp = createDistCp(srcs, maxTasks, maxBandwidthMB, dstFsConf);
        if (!distCp.execute().isSuccessful()) {
          throw new CrunchRuntimeException("Distributed copy failed from " + srcPattern + " to " + path);
        }
        LOG.info("Distributed copy completed for {} files", srcs.length);
      } catch (Exception e) {
        throw new CrunchRuntimeException("Distributed copy failed from " + srcPattern + " to " + path, e);
      }
    } else {
      LOG.info("No files found to distributed copy at {}", srcPattern);
    }
    dstFs.create(getSuccessIndicator(), true).close();
    LOG.info("Created success indicator file");
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

  protected DistCp createDistCp(Path[] srcs, int maxTasks, int maxBandwidthMB, Configuration conf) throws Exception {
    LOG.info("Distributed copying {} files using at most {} tasks and bandwidth limit of {} MB/s per task",
        new Object[]{srcs.length, maxTasks, maxBandwidthMB});

    Configuration distCpConf = new Configuration(conf);

    // Remove unnecessary and problematic properties from the DistCp configuration. This is necessary since
    // files referenced by these properties may have already been deleted when the DistCp is being started.
    distCpConf.unset("mapreduce.job.cache.files");
    distCpConf.unset("mapreduce.job.classpath.files");
    distCpConf.unset("tmpjars");

    // Setup renaming for part files
    List<String> renames = Lists.newArrayList();
    for (Path s : srcs) {
      Path d = getDestFile(conf, s, path, s.getName().contains("-m-"));
      renames.add(s.getName() + ":" + d.getName());
    }
    distCpConf.setStrings(CrunchRenameCopyListing.DISTCP_PATH_RENAMES, renames.toArray(new String[renames.size()]));
    distCpConf.setClass(DistCpConstants.CONF_LABEL_COPY_LISTING_CLASS, CrunchRenameCopyListing.class, CopyListing.class);

    // Once https://issues.apache.org/jira/browse/HADOOP-15281 is available, we can use the direct write
    // distcp optimization if the target path is in S3
    DistCpOptions options = new DistCpOptions(Arrays.asList(srcs), path);
    options.setMaxMaps(maxTasks);
    options.setMapBandwidth(maxBandwidthMB);
    options.setBlocking(true);

    return new DistCp(distCpConf, options);
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
    return Objects.equals(path, o.path) && Objects.equals(formatBundle, o.formatBundle);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(path).append(formatBundle).toHashCode();
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

  private Configuration getEffectiveBundleConfig(Configuration configuration) {
    // overlay the bundle config on top of a copy of the supplied config
    return formatBundle.configure(new Configuration(configuration));
  }

  @Override
  public boolean handleExisting(WriteMode strategy, long lastModForSource, Configuration conf) {
    FileSystem fs = null;
    try {
      fs = path.getFileSystem(getEffectiveBundleConfig(conf));
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
        // Last modified time is only relevant when the path exists and the
        // write mode is checkpoint
        if (successful && strategy == WriteMode.CHECKPOINT) {
          lastModForTarget = SourceTargetHelper.getLastModifiedAt(fs, path);
        }
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
