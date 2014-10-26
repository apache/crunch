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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import org.apache.commons.codec.binary.Hex;
import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.ReadableData;
import org.apache.crunch.io.SourceTargetHelper;
import org.apache.crunch.io.impl.FileSourceImpl;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.KeyValueSerialization;
import org.apache.hadoop.hbase.mapreduce.MutationSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class HFileSource extends FileSourceImpl<KeyValue> implements ReadableSource<KeyValue> {

  private static final Logger LOG = LoggerFactory.getLogger(HFileSource.class);
  private static final PType<KeyValue> KEY_VALUE_PTYPE = HBaseTypes.keyValues();

  public HFileSource(Path path) {
    this(ImmutableList.of(path));
  }

  public HFileSource(List<Path> paths) {
    this(paths, new Scan());
  }

  // Package-local. Don't want it to be too open, because we only support limited filters yet
  // (namely start/stop row). Users who need advanced filters should use HFileUtils#scanHFiles.
  HFileSource(List<Path> paths, Scan scan) {
    super(paths, KEY_VALUE_PTYPE, createInputFormatBundle(scan)
        // "combine file" is not supported by HFileInputFormat, as it overrides listStatus().
        .set(RuntimeParameters.DISABLE_COMBINE_FILE, "true"));
  }

  private static FormatBundle<HFileInputFormat> createInputFormatBundle(Scan scan) {
    FormatBundle<HFileInputFormat> bundle = FormatBundle.forInput(HFileInputFormat.class);
    if (!Objects.equal(scan.getStartRow(), HConstants.EMPTY_START_ROW)) {
      bundle.set(HFileInputFormat.START_ROW_KEY, Hex.encodeHexString(scan.getStartRow()));
    }
    if (!Objects.equal(scan.getStopRow(), HConstants.EMPTY_END_ROW)) {
      bundle.set(HFileInputFormat.STOP_ROW_KEY, Hex.encodeHexString(scan.getStopRow()));
    }
    return bundle;
  }

  @Override
  public void configureSource(Job job, int inputId) throws IOException {
    TableMapReduceUtil.addDependencyJars(job);
    Configuration conf = job.getConfiguration();
    conf.setStrings("io.serializations", conf.get("io.serializations"),
        MutationSerialization.class.getName(), ResultSerialization.class.getName(),
        KeyValueSerialization.class.getName());
    super.configureSource(job, inputId);
  }

  @Override
  public Iterable<KeyValue> read(Configuration conf) throws IOException {
    conf = new Configuration(conf);
    inputBundle.configure(conf);
    if (conf.get(HFileInputFormat.START_ROW_KEY) != null ||
        conf.get(HFileInputFormat.STOP_ROW_KEY) != null) {
      throw new IllegalStateException("Cannot filter row ranges in HFileSource.read");
    }
    return read(conf, new HFileReaderFactory());
  }

  @Override
  public ReadableData<KeyValue> asReadable() {
    return new HFileReadableData(paths);
  }

  public Converter<?, ?, ?, ?> getConverter() {
    return new HBaseValueConverter<KeyValue>(KeyValue.class);
  }

  @Override
  public String toString() {
    return "HFile(" + pathsAsString() + ")";
  }

  @Override
  public long getSize(Configuration conf) {
    // HFiles are stored into <family>/<file>, but the default implementation does not support this.
    // This is used for estimating the number of reducers. (Otherwise we will always get 1 reducer.)
    long sum = 0;
    for (Path path : getPaths()) {
      try {
        sum += getSizeInternal(conf, path);
      } catch (IOException e) {
        LOG.warn("Failed to estimate size of {}", path);
      }
      LOG.info("Size after read of path = {} = {}", path.toString(), sum);
    }
    return sum;
  }

  private long getSizeInternal(Configuration conf, Path path) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    FileStatus[] statuses = fs.globStatus(path, HFileInputFormat.HIDDEN_FILE_FILTER);
    if (statuses == null) {
      return 0;
    }
    long sum = 0;
    for (FileStatus status : statuses) {
      if (status.isDir()) {
        sum += SourceTargetHelper.getPathSize(fs, status.getPath());
      } else {
        sum += status.getLen();
      }
    }
    return sum;
  }
}
