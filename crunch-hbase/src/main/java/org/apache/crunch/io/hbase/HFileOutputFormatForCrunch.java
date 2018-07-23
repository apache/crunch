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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.HFileWriterImpl;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  // HCOLUMN_DESCRIPTOR_KEY is no longer used, but left for binary compatibility
  public static final String HCOLUMN_DESCRIPTOR_KEY = "hbase.hfileoutputformat.column.descriptor";
  public static final String HCOLUMN_DESCRIPTOR_COMPRESSION_TYPE_KEY = "hbase.hfileoutputformat.column.descriptor.compressiontype";
  public static final String HCOLUMN_DESCRIPTOR_DATA_BLOCK_ENCODING_KEY = "hbase.hfileoutputformat.column.descriptor.datablockencoding";
  public static final String HCOLUMN_DESCRIPTOR_BLOOM_FILTER_TYPE_KEY = "hbase.hfileoutputformat.column.descriptor.bloomfiltertype";
  private static final String COMPACTION_EXCLUDE_CONF_KEY = "hbase.mapreduce.hfileoutputformat.compaction.exclude";
  private static final Logger LOG = LoggerFactory.getLogger(HFileOutputFormatForCrunch.class);

  private final byte [] now = Bytes.toBytes(System.currentTimeMillis());

  @Override
  public RecordWriter<Object, Cell> getRecordWriter(final TaskAttemptContext context)
      throws IOException, InterruptedException {
    Path outputPath = getDefaultWorkFile(context, "");
    Configuration conf = context.getConfiguration();
    FileSystem fs = outputPath.getFileSystem(conf);

    final boolean compactionExclude = conf.getBoolean(
        COMPACTION_EXCLUDE_CONF_KEY, false);

    LOG.info("Output path: {}", outputPath);
    Configuration noCacheConf = new Configuration(conf);
    noCacheConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f);
    StoreFileWriter.Builder writerBuilder =
        new StoreFileWriter.Builder(conf, new CacheConfig(noCacheConf), fs)
        .withComparator(CellComparatorImpl.COMPARATOR)
        .withFilePath(outputPath)
        .withFileContext(getContext(conf));
    String bloomFilterType = conf.get(HCOLUMN_DESCRIPTOR_BLOOM_FILTER_TYPE_KEY);
    if (bloomFilterType != null) {
      writerBuilder.withBloomType(BloomType.valueOf(bloomFilterType));
    }
    final StoreFileWriter writer = writerBuilder.build();

    return new RecordWriter<Object, Cell>() {

      long maxSeqId = 0L;

      @Override
      public void write(Object row, Cell cell)
          throws IOException {
        KeyValue copy = KeyValueUtil.copyToNewKeyValue(cell);
        if (copy.getTimestamp() == HConstants.LATEST_TIMESTAMP) {
          copy.updateLatestStamp(now);
        }
        writer.append(copy);
        long seqId = cell.getSequenceId();
        if (seqId > maxSeqId) {
          maxSeqId = seqId;
        }
      }

      @Override
      public void close(TaskAttemptContext c) throws IOException {
        // true => product of major compaction
        writer.appendMetadata(maxSeqId, true);
        writer.appendFileInfo(HStoreFile.BULKLOAD_TIME_KEY,
            Bytes.toBytes(System.currentTimeMillis()));
        writer.appendFileInfo(HStoreFile.BULKLOAD_TASK_KEY,
            Bytes.toBytes(context.getTaskAttemptID().toString()));
        writer.appendFileInfo(HStoreFile.EXCLUDE_FROM_MINOR_COMPACTION_KEY,
            Bytes.toBytes(compactionExclude));
        writer.close();
      }
    };
  }

  private HFileContext getContext(Configuration conf) {
    HFileContextBuilder contextBuilder = new HFileContextBuilder();
    String compressionType = conf.get(HCOLUMN_DESCRIPTOR_COMPRESSION_TYPE_KEY);
    if (compressionType != null) {
      contextBuilder.withCompression(HFileWriterImpl.compressionByName(compressionType));
    }
    String dataBlockEncoding = conf.get(HCOLUMN_DESCRIPTOR_DATA_BLOCK_ENCODING_KEY);
    if (dataBlockEncoding != null) {
      contextBuilder.withDataBlockEncoding(DataBlockEncoding.valueOf(dataBlockEncoding));
    }
    return contextBuilder.build();
  }
}
