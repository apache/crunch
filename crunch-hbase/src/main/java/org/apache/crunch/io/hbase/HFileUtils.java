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
import com.google.common.collect.Lists;
import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.crunch.FilterFn;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.sort.TotalOrderPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;
import java.util.List;

import static org.apache.crunch.types.writable.Writables.nulls;
import static org.apache.crunch.types.writable.Writables.tableOf;
import static org.apache.crunch.types.writable.Writables.writables;

public final class HFileUtils {

  private static final Log LOG = LogFactory.getLog(HFileUtils.class);

  private static class FilterByFamilyFn extends FilterFn<KeyValue> {

    private final byte[] family;

    private FilterByFamilyFn(byte[] family) {
      this.family = family;
    }

    @Override
    public boolean accept(KeyValue input) {
      return Bytes.equals(
          input.getBuffer(), input.getFamilyOffset(), input.getFamilyLength(),
          family, 0, family.length);
    }
  }

  private static class KeyValueComparator implements RawComparator<KeyValue> {

    @Override
    public int compare(byte[] left, int loffset, int llength, byte[] right, int roffset, int rlength) {
      // BytesWritable serialize length in first 4 bytes.
      // We simply ignore it here, because KeyValue has its own size serialized.
      if (llength < 4) {
        throw new AssertionError("Too small llength: " + llength);
      }
      if (rlength < 4) {
        throw new AssertionError("Too small rlength: " + rlength);
      }
      KeyValue leftKey = new KeyValue(left, loffset + 4, llength - 4);
      KeyValue rightKey = new KeyValue(right, roffset + 4, rlength - 4);
      return compare(leftKey, rightKey);
    }

    @Override
    public int compare(KeyValue left, KeyValue right) {
      return KeyValue.COMPARATOR.compare(left, right);
    }
  }

  private HFileUtils() {}

  public static void writeToHFilesForIncrementalLoad(
      PCollection<KeyValue> kvs,
      HTable table,
      Path outputPath) throws IOException {
    HColumnDescriptor[] families = table.getTableDescriptor().getColumnFamilies();
    if (families.length == 0) {
      LOG.warn(table + "has no column families");
      return;
    }
    for (HColumnDescriptor f : families) {
      byte[] family = f.getName();
      PCollection<KeyValue> sorted = sortAndPartition(
          kvs.filter(new FilterByFamilyFn(family)), table);
      sorted.write(new HFileTarget(new Path(outputPath, Bytes.toString(family))));
    }
  }

  public static PCollection<KeyValue> sortAndPartition(PCollection<KeyValue> kvs, HTable table) throws IOException {
    Configuration conf = kvs.getPipeline().getConfiguration();
    PTable<KeyValue, Void> t = kvs.parallelDo(new MapFn<KeyValue, Pair<KeyValue, Void>>() {
      @Override
      public Pair<KeyValue, Void> map(KeyValue input) {
        return Pair.of(input, (Void) null);
      }
    }, tableOf(writables(KeyValue.class), nulls()));
    List <KeyValue> splitPoints = getSplitPoints(table);
    Path partitionFile = new Path(((MRPipeline) kvs.getPipeline()).createTempPath(), "partition");
    writePartitionInfo(conf, partitionFile, splitPoints);
    TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
    GroupingOptions options = GroupingOptions.builder()
        .partitionerClass(TotalOrderPartitioner.class)
        .numReducers(splitPoints.size() + 1)
        .sortComparatorClass(KeyValueComparator.class)
        .build();
    return t.groupByKey(options).ungroup().keys();
  }

  private static List<KeyValue> getSplitPoints(HTable table) throws IOException {
    List<byte[]> startKeys = ImmutableList.copyOf(table.getStartKeys());
    if (startKeys.isEmpty()) {
      throw new AssertionError(table + " has no regions!");
    }
    List<KeyValue> splitPoints = Lists.newArrayList();
    for (byte[] startKey : startKeys.subList(1, startKeys.size())) {
      KeyValue kv = KeyValue.createFirstOnRow(startKey);
      LOG.debug("split row: " + Bytes.toString(kv.getRow()));
      splitPoints.add(kv);
    }
    return splitPoints;
  }

  private static void writePartitionInfo(
      Configuration conf,
      Path path,
      List<KeyValue> splitPoints) throws IOException {
    LOG.info("Writing " + splitPoints.size() + " split points to " + path);
    SequenceFile.Writer writer = SequenceFile.createWriter(
        path.getFileSystem(conf),
        conf,
        path,
        NullWritable.class,
        KeyValue.class);
    for (KeyValue key : splitPoints) {
      writer.append(NullWritable.get(), writables(KeyValue.class).getOutputMapFn().map(key));
    }
    writer.close();
  }
}
