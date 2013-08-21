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

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoderImpl;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.TimeRangeTracker;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * This is a thin wrapper of {@link HFile.Writer}. It only calls {@link HFile.Writer#append(byte[], byte[])}
 * when records are emitted. It only supports writing data into a single column family. Records MUST be sorted
 * by their column qualifier, then timestamp reversely. All data are written into a single HFile.
 *
 * HBase's official {@code HFileOutputFormat} is not used, because it shuffles on row-key only and
 * does in-memory sort at reducer side (so the size of output HFile is limited to reducer's memory).
 * As crunch supports more complex and flexible MapReduce pipeline, we would prefer thin and pure
 * {@code OutputFormat} here.
 */
public class HFileOutputFormatForCrunch extends FileOutputFormat<Object, KeyValue> {

  public static final String HCOLUMN_DESCRIPTOR_KEY = "hbase.hfileoutputformat.column.descriptor";
  private static final String COMPACTION_EXCLUDE_CONF_KEY = "hbase.mapreduce.hfileoutputformat.compaction.exclude";
  private static final Log LOG = LogFactory.getLog(HFileOutputFormatForCrunch.class);

  private final byte [] now = Bytes.toBytes(System.currentTimeMillis());
  private final TimeRangeTracker trt = new TimeRangeTracker();

  public RecordWriter<Object, KeyValue> getRecordWriter(final TaskAttemptContext context)
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
    LOG.info("Output path: " + outputPath);
    LOG.info("HColumnDescriptor: " + hcol.toString());
    final HFile.Writer writer = HFile.getWriterFactoryNoCache(conf)
        .withPath(fs, outputPath)
        .withBlockSize(hcol.getBlocksize())
        .withCompression(hcol.getCompression())
        .withComparator(KeyValue.KEY_COMPARATOR)
        .withDataBlockEncoder(new HFileDataBlockEncoderImpl(hcol.getDataBlockEncoding()))
        .withChecksumType(Store.getChecksumType(conf))
        .withBytesPerChecksum(Store.getBytesPerChecksum(conf))
        .create();

    return new RecordWriter<Object, KeyValue>() {
      public void write(Object row, KeyValue kv)
          throws IOException {
        if (kv.getTimestamp() == HConstants.LATEST_TIMESTAMP) {
          kv.updateLatestStamp(now);
        }
        writer.append(kv);
        trt.includeTimestamp(kv);
      }

      public void close(TaskAttemptContext c)
          throws IOException, InterruptedException {
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
}
