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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.fs.HFileSystem;
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
import java.net.InetSocketAddress;

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
    final Configuration conf = context.getConfiguration();
    FileSystem fs = new HFileSystem(outputPath.getFileSystem(conf));

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
    final StoreFile.WriterBuilder writerBuilder = new StoreFile.WriterBuilder(conf, new CacheConfig(noCacheConf), fs)
        .withComparator(KeyValue.COMPARATOR)
        .withFileContext(getContext(hcol))
        .withFilePath(outputPath)
        .withBloomType(hcol.getBloomFilterType());

    return new RecordWriter<Object, Cell>() {

      StoreFile.Writer writer = null;

      @Override
      public void write(Object row, Cell cell)
          throws IOException {

        if (writer == null) {
          writer = writerBuilder
              .withFavoredNodes(getPreferredNodes(conf, cell))
              .build();
        }

        KeyValue copy = KeyValue.cloneAndAddTags(cell, ImmutableList.<Tag>of());
        if (copy.getTimestamp() == HConstants.LATEST_TIMESTAMP) {
          copy.updateLatestStamp(now);
        }
        writer.append(copy);
        trt.includeTimestamp(copy);
      }

      @Override
      public void close(TaskAttemptContext c) throws IOException {
        if (writer != null) {
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
      }
    };
  }

  /**
   * Returns the "preferred" node for the given cell, or null if no preferred node can be found. The "preferred"
   * node for a cell is defined as the host where the region server is located that is hosting the region that will
   * contain the given cell.
   */
  private InetSocketAddress[] getPreferredNodes(Configuration conf, Cell cell) throws IOException {
    String regionLocationFilePathStr = conf.get(RegionLocationTable.REGION_LOCATION_TABLE_PATH);
    if (regionLocationFilePathStr != null) {
      LOG.debug("Reading region location file from {}", regionLocationFilePathStr);
      Path regionLocationPath = new Path(regionLocationFilePathStr);
      try (FSDataInputStream inputStream = regionLocationPath.getFileSystem(conf).open(regionLocationPath)) {
        RegionLocationTable regionLocationTable = RegionLocationTable.deserialize(inputStream);
        InetSocketAddress preferredNodeForRow = regionLocationTable.getPreferredNodeForRow(CellUtil.cloneRow(cell));
        if (preferredNodeForRow != null) {
          return new InetSocketAddress[] { preferredNodeForRow };
        } else {
          return null;
        }
      }
    } else {
      LOG.warn("No region location file path found in configuration");
      return null;
    }
  }

  private HFileContext getContext(HColumnDescriptor desc) {
    HFileContext ctxt = new HFileContext();
    ctxt.setDataBlockEncoding(desc.getDataBlockEncoding());
    ctxt.setCompression(desc.getCompression());
    return ctxt;
  }
}
