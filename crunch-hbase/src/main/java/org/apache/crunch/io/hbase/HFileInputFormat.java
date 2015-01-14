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

import com.google.common.collect.ImmutableList;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple input format for HFiles.
 */
public class HFileInputFormat extends FileInputFormat<NullWritable, KeyValue> {

  private static final Logger LOG = LoggerFactory.getLogger(HFileInputFormat.class);
  static final String START_ROW_KEY = "crunch.hbase.hfile.input.format.start.row";
  static final String STOP_ROW_KEY = "crunch.hbase.hfile.input.format.stop.row";

  /**
   * File filter that removes all "hidden" files. This might be something worth removing from
   * a more general purpose utility; it accounts for the presence of metadata files created
   * in the way we're doing exports.
   */
  static final PathFilter HIDDEN_FILE_FILTER = new PathFilter() {
    public boolean accept(Path p) {
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  /**
   * Record reader for HFiles.
   */
  private static class HFileRecordReader extends RecordReader<NullWritable, KeyValue> {

    private Reader in;
    protected Configuration conf;
    private HFileScanner scanner;

    /**
     * A private cache of the key value so it doesn't need to be loaded twice from the scanner.
     */
    private KeyValue value = null;
    private byte[] startRow = null;
    private byte[] stopRow = null;
    private boolean reachedStopRow = false;
    private long count;
    private boolean seeked = false;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      FileSplit fileSplit = (FileSplit) split;
      conf = context.getConfiguration();
      Path path = fileSplit.getPath();
      FileSystem fs = path.getFileSystem(conf);
      LOG.info("Initialize HFileRecordReader for {}", path);
      this.in = HFile.createReader(fs, path, new CacheConfig(conf), conf);

      // The file info must be loaded before the scanner can be used.
      // This seems like a bug in HBase, but it's easily worked around.
      this.in.loadFileInfo();
      this.scanner = in.getScanner(false, false);

      String startRowStr = conf.get(START_ROW_KEY);
      if (startRowStr != null) {
        this.startRow = decodeHexOrDie(startRowStr);
      }
      String stopRowStr = conf.get(STOP_ROW_KEY);
      if (stopRowStr != null) {
        this.stopRow = decodeHexOrDie(stopRowStr);
      }
    }

    private static byte[] decodeHexOrDie(String s) {
      try {
        return Hex.decodeHex(s.toCharArray());
      } catch (DecoderException e) {
        throw new AssertionError("Failed to decode hex string: " + s);
      }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (reachedStopRow) {
        return false;
      }
      boolean hasNext;
      if (!seeked) {
        if (startRow != null) {
          if(LOG.isInfoEnabled()) {
            LOG.info("Seeking to start row {}", Bytes.toStringBinary(startRow));
          }
          KeyValue kv = KeyValue.createFirstOnRow(startRow);
          hasNext = seekAtOrAfter(scanner, kv);
        } else {
          LOG.info("Seeking to start");
          hasNext = scanner.seekTo();
        }
        seeked = true;
      } else {
        hasNext = scanner.next();
      }
      if (!hasNext) {
        return false;
      }
      value = KeyValue.cloneAndAddTags(scanner.getKeyValue(), ImmutableList.<Tag>of());
      if (stopRow != null && Bytes.compareTo(CellUtil.cloneRow(value), stopRow) >= 0) {
        if(LOG.isInfoEnabled()) {
          LOG.info("Reached stop row {}", Bytes.toStringBinary(stopRow));
        }
        reachedStopRow = true;
        value = null;
        return false;
      }
      count++;
      return true;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
      return NullWritable.get();
    }

    @Override
    public KeyValue getCurrentValue() throws IOException, InterruptedException {
      return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      // This would be inaccurate if KVs are not uniformly-sized or we have performed a seek to
      // the start row, but better than nothing anyway.
      return 1.0f * count / in.getEntries();
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    // This method is copied from o.a.h.hbase.regionserver.StoreFileScanner, as we don't want
    // to depend on it.
    private static boolean seekAtOrAfter(HFileScanner s, KeyValue k)
        throws IOException {
      int result = s.seekTo(k.getBuffer(), k.getKeyOffset(), k.getKeyLength());
      if(result < 0) {
        // Passed KV is smaller than first KV in file, work from start of file
        return s.seekTo();
      } else if(result > 0) {
        // Passed KV is larger than current KV in file, if there is a next
        // it is the "after", if not then this scanner is done.
        return s.next();
      }
      // Seeked to the exact key
      return true;
    }
  }

  @Override
  protected List<FileStatus> listStatus(JobContext job) throws IOException {
    List<FileStatus> result = new ArrayList<FileStatus>();

    // Explode out directories that match the original FileInputFormat filters since HFiles are written to directories where the
    // directory name is the column name
    for (FileStatus status : super.listStatus(job)) {
      if (status.isDir()) {
        FileSystem fs = status.getPath().getFileSystem(job.getConfiguration());
        for (FileStatus match : fs.listStatus(status.getPath(), HIDDEN_FILE_FILTER)) {
          result.add(match);
        }
      }
      else{
        result.add(status);
      }
    }

    return result;
  }

  @Override
  public RecordReader<NullWritable, KeyValue> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new HFileRecordReader();
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    // This file isn't splittable.
    return false;
  }
}