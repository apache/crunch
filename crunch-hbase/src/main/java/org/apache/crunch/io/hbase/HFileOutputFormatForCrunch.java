/**
 * Copyright 2009 The Apache Software Foundation
 *
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

import com.google.common.collect.ImmutableList;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.TimeRangeTracker;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * This is a thin wrapper of {@link HFile.Writer}. It only calls {@link HFile.Writer#append}
 * when records are emitted. It only supports writing data into a single column family. Records MUST be sorted
 * by their column qualifier, then timestamp reversely. All data are written into a single HFile.
 *
 * HBase's official {@code HFileOutputFormat} is not used, because it shuffles on row-key only and
 * does in-memory sort at reducer side (so the size of output HFile is limited to reducer's memory).
 * As crunch supports more complex and flexible MapReduce pipeline, we would prefer thin and pure
 * {@code OutputFormat} here.
 */
public class HFileOutputFormatForCrunch extends FileOutputFormat<Object, Cell> {

  public static final String HCOLUMN_DESCRIPTOR_KEY = "hbase.hfileoutputformat.column.descriptor";
  private static final String COMPACTION_EXCLUDE_CONF_KEY = "hbase.mapreduce.hfileoutputformat.compaction.exclude";
  private static final Logger LOG = LoggerFactory.getLogger(HFileOutputFormatForCrunch.class);

  private final byte [] now = Bytes.toBytes(System.currentTimeMillis());
  private final TimeRangeTracker trt = new TimeRangeTracker();

  @Override
  public RecordWriter<Object, Cell> getRecordWriter(final TaskAttemptContext context)
      throws IOException, InterruptedException {
    Path outputPath = getDefaultWorkFile(context, "");
    Configuration conf = context.getConfiguration();
    FileSystem fs = outputPath.getFileSystem(conf);

    final boolean compactionExclude = conf.getBoolean(
        COMPACTION_EXCLUDE_CONF_KEY, false);

    String hcolStr = conf.get(HCOLUMN_DESCRIPTOR_KEY);
    if (hcolStr == null) {
      throw new AssertionError(HCOLUMN_DESCRIPTOR_KEY + " is not set in conf");
    }
    byte[] hcolBytes;
    try {
      hcolBytes = Hex.decodeHex(hcolStr.toCharArray());
    } catch (DecoderException e) {
      throw new AssertionError("Bad hex string: " + hcolStr);
    }
    HColumnDescriptor hcol = new HColumnDescriptor();
    hcol.readFields(new DataInputStream(new ByteArrayInputStream(hcolBytes)));
    LOG.info("Output path: {}", outputPath);
    LOG.info("HColumnDescriptor: {}", hcol.toString());
    Configuration noCacheConf = new Configuration(conf);
    noCacheConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f);
    final StoreFile.Writer writer = new StoreFile.WriterBuilder(conf, new CacheConfig(noCacheConf), fs)
        .withComparator(KeyValue.COMPARATOR)
        .withFileContext(getContext(hcol))
        .withFilePath(outputPath)
        .withBloomType(hcol.getBloomFilterType())
        .build();

    return new RecordWriter<Object, Cell>() {
      @Override
      public void write(Object row, Cell cell)
          throws IOException {
        KeyValue copy = KeyValue.cloneAndAddTags(cell, ImmutableList.<Tag>of());
        if (copy.getTimestamp() == HConstants.LATEST_TIMESTAMP) {
          copy.updateLatestStamp(now);
        }
        writer.append(copy);
        trt.includeTimestamp(copy);
      }

      @Override
      public void close(TaskAttemptContext c) throws IOException {
        writer.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY,
            Bytes.toBytes(System.currentTimeMillis()));
        writer.appendFileInfo(StoreFile.BULKLOAD_TASK_KEY,
            Bytes.toBytes(context.getTaskAttemptID().toString()));
        writer.appendFileInfo(StoreFile.MAJOR_COMPACTION_KEY,
            Bytes.toBytes(true));
        writer.appendFileInfo(StoreFile.EXCLUDE_FROM_MINOR_COMPACTION_KEY,
            Bytes.toBytes(compactionExclude));
        writer.appendFileInfo(StoreFile.TIMERANGE_KEY,
            WritableUtils.toByteArray(trt));
        writer.close();
      }
    };
  }

  private HFileContext getContext(HColumnDescriptor desc) {
    HFileContext ctxt = new HFileContext();
    ctxt.setDataBlockEncoding(desc.getDataBlockEncoding());
    ctxt.setCompression(desc.getCompression());
    return ctxt;
  }
}
