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
package org.apache.crunch.io.hbase;

import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.crunch.io.SequentialFileNamingScheme;
import org.apache.crunch.io.impl.FileTargetImpl;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.KeyValueSerialization;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public class HFileTarget extends FileTargetImpl {

  private static final Logger LOG = LoggerFactory.getLogger(HFileTarget.class);

  public HFileTarget(String path) {
    this(new Path(path));
  }

  public HFileTarget(Path path) {
    this(path, null);
  }

  public HFileTarget(Path path, HColumnDescriptor hcol) {
    super(path, HFileOutputFormatForCrunch.class, SequentialFileNamingScheme.getInstance());
    if (hcol != null) {
      outputConf(HFileOutputFormatForCrunch.HCOLUMN_DESCRIPTOR_COMPRESSION_TYPE_KEY,
          hcol.getCompressionType().getName());
      outputConf(HFileOutputFormatForCrunch.HCOLUMN_DESCRIPTOR_DATA_BLOCK_ENCODING_KEY,
          hcol.getDataBlockEncoding().name());
      outputConf(HFileOutputFormatForCrunch.HCOLUMN_DESCRIPTOR_BLOOM_FILTER_TYPE_KEY,
          hcol.getBloomFilterType().name());
    }
  }

  @Override
  public void configureForMapReduce(Job job, PType<?> ptype, Path outputPath, String name) {
    Configuration conf = job.getConfiguration();
    HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));
    conf.setStrings("io.serializations", conf.get("io.serializations"),
        KeyValueSerialization.class.getName());
    super.configureForMapReduce(job, ptype, outputPath, name);
  }

  @Override
  public Converter<?, ?, ?, ?> getConverter(PType<?> ptype) {
    PType<?> valueType = ptype;
    if (ptype instanceof PTableType) {
      valueType = ((PTableType) ptype).getValueType();
    }
    if (!Cell.class.isAssignableFrom(valueType.getTypeClass())) {
      throw new IllegalArgumentException("HFileTarget only supports Cell outputs");
    }
    if (ptype instanceof PTableType) {
      return new HBasePairConverter<ImmutableBytesWritable, Cell>(ImmutableBytesWritable.class, Cell.class);
    }
    return new HBaseValueConverter<Cell>(Cell.class);
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

    if (!sameFs) {
      if (srcs.length > 0) {
        int maxDistributedCopyTasks = conf.getInt(RuntimeParameters.FILE_TARGET_MAX_DISTCP_TASKS, 1000);
        LOG.info(
                "Source and destination are in different file systems, performing distcp of {} files from [{}] to [{}] "
                        + "using at most {} tasks",
                new Object[] { srcs.length, src, path, maxDistributedCopyTasks });
        // Once https://issues.apache.org/jira/browse/HADOOP-15281 is available, we can use the direct write
        // distcp optimization if the target path is in S3
        DistCpOptions options = new DistCpOptions(Arrays.asList(srcs), path);
        options.setMaxMaps(maxDistributedCopyTasks);
        options.setOverwrite(true);
        options.setBlocking(true);

        Configuration distCpConf = new Configuration(conf);
        // Remove unnecessary and problematic properties from the DistCp configuration. This is necessary since
        // files referenced by these properties may have already been deleted when the DistCp is being started.
        distCpConf.unset("mapreduce.job.cache.files");
        distCpConf.unset("mapreduce.job.classpath.files");
        distCpConf.unset("tmpjars");

        try {
          DistCp distCp = new DistCp(distCpConf, options);
          if (!distCp.execute().isSuccessful()) {
            throw new CrunchRuntimeException("Unable to move files through distcp from " + src + " to " + path);
          }
          LOG.info("Distributed copy completed for {} files", srcs.length);
        } catch (Exception e) {
          throw new CrunchRuntimeException("Unable to move files through distcp from " + src + " to " + path, e);
        }
      } else {
        LOG.info("No files found at [{}], not attempting to copy HFiles", src);
      }
    } else {
      LOG.info(
              "Source and destination are in the same file system, performing rename of {} files from [{}] to [{}]",
              new Object[] { srcs.length, src, path });

      for (Path s : srcs) {
        Path d = getDestFile(conf, s, path, s.getName().contains("-m-"));
        srcFs.rename(s, d);
      }
    }
    dstFs.create(getSuccessIndicator(), true).close();
    LOG.info("Created success indicator file");
  }

  @Override
  public String toString() {
    return "HFile(" + path + ")";
  }
}
