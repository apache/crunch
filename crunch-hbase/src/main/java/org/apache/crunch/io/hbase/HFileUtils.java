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

import static org.apache.crunch.types.writable.Writables.bytes;
import static org.apache.crunch.types.writable.Writables.nulls;
import static org.apache.crunch.types.writable.Writables.tableOf;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.FilterFn;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.dist.DistributedPipeline;
import org.apache.crunch.lib.sort.TotalOrderPartitioner;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HFileUtils {

  private static final Logger LOG = LoggerFactory.getLogger(HFileUtils.class);

  /** Compares {@code KeyValue} by its family, qualifier, timestamp (reversely), type (reversely) and memstoreTS. */
  private static final Comparator<KeyValue> KEY_VALUE_COMPARATOR = new Comparator<KeyValue>() {
    @Override
    public int compare(KeyValue l, KeyValue r) {
      int cmp;
      if ((cmp = compareFamily(l, r)) != 0) {
        return cmp;
      }
      if ((cmp = compareQualifier(l, r)) != 0) {
        return cmp;
      }
      if ((cmp = compareTimestamp(l, r)) != 0) {
        return cmp;
      }
      if ((cmp = compareType(l, r)) != 0) {
        return cmp;
      }
      return 0;
    }

    private int compareFamily(KeyValue l, KeyValue r) {
      return Bytes.compareTo(
          l.getBuffer(), l.getFamilyOffset(), l.getFamilyLength(),
          r.getBuffer(), r.getFamilyOffset(), r.getFamilyLength());
    }

    private int compareQualifier(KeyValue l, KeyValue r) {
      return Bytes.compareTo(
          l.getBuffer(), l.getQualifierOffset(), l.getQualifierLength(),
          r.getBuffer(), r.getQualifierOffset(), r.getQualifierLength());
    }

    private int compareTimestamp(KeyValue l, KeyValue r) {
      // These arguments are intentionally reversed, with r then l, to sort
      // the timestamps in descending order as is expected by HBase
      return Longs.compare(r.getTimestamp(), l.getTimestamp());
    }

    private int compareType(KeyValue l, KeyValue r) {
      return (int) r.getTypeByte() - (int) l.getTypeByte();
    }

  };

  private static class FilterByFamilyFn<C extends Cell> extends FilterFn<C> {

    private final byte[] family;

    private FilterByFamilyFn(byte[] family) {
      this.family = family;
    }

    @Override
    public boolean accept(C input) {
      return Bytes.equals(
          input.getFamilyArray(), input.getFamilyOffset(), input.getFamilyLength(),
          family, 0, family.length);
    }

    @Override
    public boolean disableDeepCopy() {
      return true;
    }
  }

  private static class StartRowFilterFn<C extends Cell> extends FilterFn<C> {

    private final byte[] startRow;

    private StartRowFilterFn(byte[] startRow) {
      this.startRow = startRow;
    }

    @Override
    public boolean accept(C input) {
      return Bytes.compareTo(
          input.getRowArray(), input.getRowOffset(), input.getRowLength(),
          startRow, 0, startRow.length) >= 0;
    }
  }

  private static class StopRowFilterFn<C extends Cell> extends FilterFn<C> {

    private final byte[] stopRow;

    private StopRowFilterFn(byte[] stopRow) {
      this.stopRow = stopRow;
    }

    @Override
    public boolean accept(C input) {
      return Bytes.compareTo(
          input.getRowArray(), input.getRowOffset(), input.getRowLength(),
          stopRow, 0, stopRow.length) < 0;
    }
  }

  private static class FamilyMapFilterFn<C extends Cell> extends FilterFn<C> {

    private static class Column implements Serializable {

      private final byte[] family;
      private final byte[] qualifier;

      private Column(byte[] family, byte[] qualifier) {
        this.family = family;
        this.qualifier = qualifier;
      }

      private byte[] getFamily() {
        return family;
      }

      private byte[] getQualifier() {
        return qualifier;
      }
    }

    private final List<byte[]> families = Lists.newArrayList();
    private final List<Column> qualifiers = Lists.newArrayList();

    private transient Set<ByteBuffer> familySet;
    private transient Set<Pair<ByteBuffer, ByteBuffer>> qualifierSet;

    private FamilyMapFilterFn(Map<byte[], NavigableSet<byte[]>> familyMap) {
      // Holds good families and qualifiers in Lists, as ByteBuffer is not Serializable.
      for (Map.Entry<byte[], NavigableSet<byte[]>> e : familyMap.entrySet()) {
        byte[] f = e.getKey();
        if (e.getValue() == null) {
          families.add(f);
        } else {
          for (byte[] q : e.getValue()) {
            qualifiers.add(new Column(f, q));
          }
        }
      }
    }

    @Override
    public void initialize() {
      ImmutableSet.Builder<ByteBuffer> familiySetBuilder = ImmutableSet.builder();
      ImmutableSet.Builder<Pair<ByteBuffer, ByteBuffer>> qualifierSetBuilder
          = ImmutableSet.builder();
      for (byte[] f : families) {
        familiySetBuilder.add(ByteBuffer.wrap(f));
      }
      for (Column e : qualifiers) {
        byte[] f = e.getFamily();
        byte[] q = e.getQualifier();
        qualifierSetBuilder.add(Pair.of(ByteBuffer.wrap(f), ByteBuffer.wrap(q)));
      }
      this.familySet = familiySetBuilder.build();
      this.qualifierSet = qualifierSetBuilder.build();
    }

    @Override
    public boolean accept(C input) {
      ByteBuffer f = ByteBuffer.wrap(input.getFamilyArray(), input.getFamilyOffset(), input.getFamilyLength());
      ByteBuffer q = ByteBuffer.wrap(input.getQualifierArray(), input.getQualifierOffset(), input.getQualifierLength());
      return familySet.contains(f) || qualifierSet.contains(Pair.of(f, q));
    }
  }

  private static class TimeRangeFilterFn<C extends Cell> extends FilterFn<C> {

    private final long minTimestamp;
    private final long maxTimestamp;

    private TimeRangeFilterFn(TimeRange timeRange) {
      // Can't save TimeRange to member directly, as it is not Serializable.
      this.minTimestamp = timeRange.getMin();
      this.maxTimestamp = timeRange.getMax();
    }

    @Override
    public boolean accept(C input) {
      return (minTimestamp <= input.getTimestamp() && input.getTimestamp() < maxTimestamp);
    }
  }

  public static class KeyValueComparator implements RawComparator<BytesWritable> {

    @Override
    public int compare(byte[] left, int loffset, int llength, byte[] right, int roffset, int rlength) {
      // BytesWritable and KeyValue each serialize 4 bytes to indicate length
      if (llength < 8) {
        throw new AssertionError("Too small llength: " + llength);
      }
      if (rlength < 8) {
        throw new AssertionError("Too small rlength: " + rlength);
      }
      Cell leftKey = new KeyValue(left, loffset + 8, llength - 8);
      Cell rightKey = new KeyValue(right, roffset + 8, rlength - 8);

      int rowCmp = Bytes.compareTo(
          leftKey.getRowArray(), leftKey.getRowOffset(), leftKey.getRowLength(),
          rightKey.getRowArray(), rightKey.getRowOffset(), rightKey.getRowLength());
      if (rowCmp != 0) {
        return rowCmp;
      } else {
        return KeyValue.COMPARATOR.compare(leftKey, rightKey);
      }
    }

    @Override
    public int compare(BytesWritable left, BytesWritable right) {
      return KeyValue.COMPARATOR.compare(
          new KeyValue(left.getBytes(), 4, left.getLength() - 4),
	  new KeyValue(right.getBytes(), 4, right.getLength() - 4));
    }
  }

  private static class ExtractRowFn<C extends Cell> extends MapFn<C, ByteBuffer> {
    @Override
    public ByteBuffer map(Cell input) {
      // we have to make a copy of row, because the buffer may be changed after this call
      return ByteBuffer.wrap(CellUtil.cloneRow(input));
    }
  }

  /**
   * Scans HFiles.
   *
   * @param pipeline the pipeline
   * @param path path to HFiles
   * @return {@code Result}s
   */
  public static PCollection<Result> scanHFiles(Pipeline pipeline, Path path) {
    return scanHFiles(pipeline, path, new Scan());
  }

  /**
   * Scans HFiles with source filesystem.
   *
   * @param pipeline the pipeline
   * @param path path to HFiles
   * @param fs filesystem where HFiles are located
   * @return {@code Result}s
   */
  public static PCollection<Result> scanHFiles(Pipeline pipeline, Path path, FileSystem fs) {
    return scanHFiles(pipeline, path, new Scan(), fs);
  }

  /**
   * Scans HFiles with filter conditions.
   *
   * @param pipeline the pipeline
   * @param path path to HFiles
   * @param scan filtering conditions
   * @return {@code Result}s
   * @see #combineIntoRow(org.apache.crunch.PCollection, org.apache.hadoop.hbase.client.Scan)
   */
  public static PCollection<Result> scanHFiles(Pipeline pipeline, Path path, Scan scan) {
    return scanHFiles(pipeline, ImmutableList.of(path), scan);
  }

  /**
   * Scans HFiles with filter conditions and source filesystem.
   *
   * @param pipeline the pipeline
   * @param path path to HFiles
   * @param scan filtering conditions
   * @param fs filesystem where HFiles are located
   * @return {@code Result}s
   * @see #combineIntoRow(org.apache.crunch.PCollection, org.apache.hadoop.hbase.client.Scan)
   */
  public static PCollection<Result> scanHFiles(Pipeline pipeline, Path path, Scan scan, FileSystem fs) {
    return scanHFiles(pipeline, ImmutableList.of(path), scan, fs);
  }

  /**
   * Scans HFiles with filter conditions.
   *
   * @param pipeline the pipeline
   * @param paths paths to HFiles
   * @param scan filtering conditions
   * @return {@code Result}s
   * @see #combineIntoRow(org.apache.crunch.PCollection, org.apache.hadoop.hbase.client.Scan)
   */
  public static PCollection<Result> scanHFiles(Pipeline pipeline, List<Path> paths, Scan scan) {
    return scanHFiles(pipeline, paths, scan, null);
  }

  /**
   * Scans HFiles with filter conditions and source filesystem.
   *
   * @param pipeline the pipeline
   * @param paths paths to HFiles
   * @param scan filtering conditions
   * @param fs filesystem where HFiles are located
   * @return {@code Result}s
   * @see #combineIntoRow(org.apache.crunch.PCollection, org.apache.hadoop.hbase.client.Scan)
   */
  public static PCollection<Result> scanHFiles(Pipeline pipeline, List<Path> paths, Scan scan, FileSystem fs) {
    PCollection<KeyValue> in = pipeline.read(new HFileSource(paths, scan).fileSystem(fs));
    return combineIntoRow(in, scan);
  }

  /**
   * Converts a bunch of {@link Cell}s into {@link Result}.
   *
   * All {@code Cell}s belong to the same row are combined. Deletes are dropped and only
   * the latest version is kept.
   *
   * @param cells the input {@code Cell}s
   * @return {@code Result}s
   */
  public static <C extends Cell> PCollection<Result> combineIntoRow(PCollection<C> cells) {
    return combineIntoRow(cells, new Scan());
  }

  /**
   * Converts a bunch of {@link Cell}s into {@link Result}.
   *
   * All {@code Cell}s belong to the same row are combined. Users may provide some filter
   * conditions (specified by {@code scan}). Deletes are dropped and only the number
   * of versions specified by {@code scan.getMaxVersions()} are kept.
   *
   * @param cells the input {@code Cell}s
   * @param scan filter conditions, currently we support start row, stop row, family map,
   *             time range, and max versions
   * @return {@code Result}s
   */
  public static <C extends Cell> PCollection<Result> combineIntoRow(PCollection<C> cells, Scan scan) {
    if (!Bytes.equals(scan.getStartRow(), HConstants.EMPTY_START_ROW)) {
      cells = cells.filter(new StartRowFilterFn<C>(scan.getStartRow()));
    }
    if (!Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW)) {
      cells = cells.filter(new StopRowFilterFn<C>(scan.getStopRow()));
    }
    if (scan.hasFamilies()) {
      cells = cells.filter(new FamilyMapFilterFn<C>(scan.getFamilyMap()));
    }
    TimeRange timeRange = scan.getTimeRange();
    if (timeRange != null && (timeRange.getMin() > 0 || timeRange.getMax() < Long.MAX_VALUE)) {
      cells = cells.filter(new TimeRangeFilterFn<C>(timeRange));
    }
    // TODO(chaoshi): support Scan#getFilter

    PTable<ByteBuffer, C> cellsByRow = cells.by(new ExtractRowFn<C>(), bytes());
    final int versions = scan.getMaxVersions();
    return cellsByRow.groupByKey().parallelDo("CombineKeyValueIntoRow",
        new DoFn<Pair<ByteBuffer, Iterable<C>>, Result>() {
          @Override
          public void process(Pair<ByteBuffer, Iterable<C>> input, Emitter<Result> emitter) {
            List<KeyValue> cells = Lists.newArrayList();
            for (Cell kv : input.second()) {
              try {
                cells.add(KeyValueUtil.copyToNewKeyValue(kv)); // assuming the input fits in memory
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
            Result result = doCombineIntoRow(cells, versions);
            if (result == null) {
              return;
            }
            emitter.emit(result);
          }
        }, HBaseTypes.results());
  }

  public static <C extends Cell> void writeToHFilesForIncrementalLoad(
          PCollection<C> cells,
          Connection connection,
          TableName tableName,
          Path outputPath) throws IOException {
    writeToHFilesForIncrementalLoad(cells, connection, tableName, outputPath, false);
  }

  public static <C extends Cell> void writeToHFilesForIncrementalLoad(
      PCollection<C> cells,
      Connection connection,
      TableName tableName,
      Path outputPath,
      FileSystem fs) throws IOException {
    writeToHFilesForIncrementalLoad(cells, connection, tableName, outputPath, false, fs);
  }

  public static <C extends Cell> void writeToHFilesForIncrementalLoad(
      PCollection<C> cells,
      Connection connection,
      TableName tableName,
      Path outputPath,
      boolean limitToAffectedRegions) throws IOException {
    writeToHFilesForIncrementalLoad(cells, connection, tableName, outputPath, limitToAffectedRegions, null);
  }

  /**
   * Writes out HFiles from the provided <code>cells</code> and <code>table</code>. <code>limitToAffectedRegions</code>
   * is used to indicate that the regions the <code>cells</code> will be loaded into should be identified prior to writing
   * HFiles. Identifying the regions ahead of time will reduce the number of reducers needed when writing. This is
   * beneficial if the data to be loaded only touches a small enough subset of the total regions in the table. If set to
   * false, the number of reducers will equal the number of regions in the table.
   *
   * @see <a href='https://issues.apache.org/jira/browse/CRUNCH-588'>CRUNCH-588</a>
   */
  public static <C extends Cell> void writeToHFilesForIncrementalLoad(
      PCollection<C> cells,
      Connection connection,
      TableName tableName,
      Path outputPath,
      boolean limitToAffectedRegions,
      FileSystem fs) throws IOException {
    Table table = connection.getTable(tableName);
    RegionLocator regionLocator = connection.getRegionLocator(tableName);
    HColumnDescriptor[] families = table.getTableDescriptor().getColumnFamilies();
    if (families.length == 0) {
      LOG.warn("{} has no column families", table);
      return;
    }
    PCollection<C> partitioned = sortAndPartition(cells, regionLocator, limitToAffectedRegions);
    RegionLocationTable regionLocationTable = RegionLocationTable.create(
        table.getName().getNameAsString(),
        regionLocator.getAllRegionLocations());
    Path regionLocationFilePath = new Path(((DistributedPipeline) cells.getPipeline()).createTempPath(),
        "regionLocations" + table.getName().getNameAsString());
     writeRegionLocationTable(cells.getPipeline().getConfiguration(), regionLocationFilePath, regionLocationTable);

    for (HColumnDescriptor f : families) {
      byte[] family = f.getName();
      partitioned
          .filter(new FilterByFamilyFn<C>(family))
          .write(new HFileTarget(new Path(outputPath, Bytes.toString(family)), f)
              .outputConf(RegionLocationTable.REGION_LOCATION_TABLE_PATH, regionLocationFilePath.toString())
              .fileSystem(fs));
    }
  }

  public static void writePutsToHFilesForIncrementalLoad(
          PCollection<Put> puts,
          Connection connection,
          TableName tableName,
          Path outputPath) throws IOException {
    writePutsToHFilesForIncrementalLoad(puts, connection, tableName, outputPath, false);
  }

  public static void writePutsToHFilesForIncrementalLoad(
      PCollection<Put> puts,
      Connection connection,
      TableName tableName,
      Path outputPath,
      FileSystem fs) throws IOException {
    writePutsToHFilesForIncrementalLoad(puts, connection, tableName, outputPath, false, fs);
  }

  public static void writePutsToHFilesForIncrementalLoad(
      PCollection<Put> puts,
      Connection connection,
      TableName tableName,
      Path outputPath,
      boolean limitToAffectedRegions) throws IOException {
    writePutsToHFilesForIncrementalLoad(puts, connection, tableName, outputPath, limitToAffectedRegions, null);
  }

  /**
   * Writes out HFiles from the provided <code>puts</code> and <code>table</code>. <code>limitToAffectedRegions</code>
   * is used to indicate that the regions the <code>puts</code> will be loaded into should be identified prior to writing
   * HFiles. Identifying the regions ahead of time will reduce the number of reducers needed when writing. This is
   * beneficial if the data to be loaded only touches a small enough subset of the total regions in the table. If set to
   * false, the number of reducers will equal the number of regions in the table.
   *
   * @see <a href='https://issues.apache.org/jira/browse/CRUNCH-588'>CRUNCH-588</a>
   */
  public static void writePutsToHFilesForIncrementalLoad(
      PCollection<Put> puts,
      Connection connection,
      TableName tableName,
      Path outputPath,
      boolean limitToAffectedRegions,
      FileSystem fs) throws IOException {
    PCollection<Cell> cells = puts.parallelDo("ConvertPutToCells", new DoFn<Put, Cell>() {
      @Override
      public void process(Put input, Emitter<Cell> emitter) {
        for (Cell cell : Iterables.concat(input.getFamilyCellMap().values())) {
          emitter.emit(cell);
        }
      }
    }, HBaseTypes.cells());
    writeToHFilesForIncrementalLoad(cells, connection, tableName, outputPath, limitToAffectedRegions, fs);
  }

  public static <C extends Cell> PCollection<C> sortAndPartition(PCollection<C> cells, RegionLocator regionLocator) throws IOException {
    return sortAndPartition(cells, regionLocator, false);
  }

  /**
   * Sorts and partitions the provided <code>cells</code> for the given <code>regionLocator</code> to ensure all elements that belong
   * in the same region end up in the same reducer. The flag <code>limitToAffectedRegions</code>, when set to true, will identify
   * the regions the data in <code>cells</code> belongs to and will set the number of reducers equal to the number of identified
   * affected regions. If set to false, then all regions will be used, and the number of reducers will be set to the number
   * of regions in the table.
   */
  public static <C extends Cell> PCollection<C> sortAndPartition(PCollection<C> cells, RegionLocator regionLocator, boolean limitToAffectedRegions) throws IOException {
    Configuration conf = cells.getPipeline().getConfiguration();
    PTable<C, Void> t = cells.parallelDo(
        "Pre-partition",
        new MapFn<C, Pair<C, Void>>() {
          @Override
          public Pair<C, Void> map(C input) {
            return Pair.of(input, (Void) null);
          }
        }, tableOf(cells.getPType(), nulls()));

    List<KeyValue> splitPoints;
    if(limitToAffectedRegions) {
      splitPoints = getSplitPoints(regionLocator, t);
    } else {
      splitPoints = getSplitPoints(regionLocator);
    }
    Path partitionFile = new Path(((DistributedPipeline) cells.getPipeline()).createTempPath(), "partition");
    writePartitionInfo(conf, partitionFile, splitPoints);
    GroupingOptions options = GroupingOptions.builder()
        .partitionerClass(TotalOrderPartitioner.class)
        .sortComparatorClass(KeyValueComparator.class)
        .conf(TotalOrderPartitioner.PARTITIONER_PATH, partitionFile.toString())
        .numReducers(splitPoints.size() + 1)
        .build();
    return t.groupByKey(options).ungroup().keys();
  }

  private static List<KeyValue> getSplitPoints(RegionLocator regionLocator) throws IOException {
    List<byte[]> startKeys = ImmutableList.copyOf(regionLocator.getStartKeys());
    if (startKeys.isEmpty()) {
      throw new AssertionError(regionLocator.getName().getNameAsString() + " has no regions!");
    }
    List<KeyValue> splitPoints = Lists.newArrayList();
    for (byte[] startKey : startKeys.subList(1, startKeys.size())) {
      KeyValue kv = KeyValueUtil.createFirstOnRow(startKey);
      LOG.debug("split row: " + Bytes.toString(CellUtil.cloneRow(kv)));
      splitPoints.add(kv);
    }
    return splitPoints;
  }

  private static <C> List<KeyValue> getSplitPoints(RegionLocator regionLocator, PTable<C, Void> affectedRows) throws IOException {
    List<byte[]> startKeys;
    try {
      startKeys = Lists.newArrayList(regionLocator.getStartKeys());
      if (startKeys.isEmpty()) {
        throw new AssertionError(regionLocator.getName().getNameAsString() + " has no regions!");
      }
    } catch (IOException e) {
      throw new CrunchRuntimeException(e);
    }

    Collections.sort(startKeys, Bytes.BYTES_COMPARATOR);

    Iterable<ByteBuffer> bufferedStartKeys = affectedRows
            .parallelDo(new DetermineAffectedRegionsFn(startKeys), Writables.bytes()).materialize();

    // set to get rid of the potential duplicate start keys emitted
    ImmutableSet.Builder<KeyValue> startKeyBldr = ImmutableSet.builder();
    for (final ByteBuffer bufferedStartKey : bufferedStartKeys) {
      startKeyBldr.add(KeyValueUtil.createFirstOnRow(bufferedStartKey.array()));
    }

    return ImmutableList.copyOf(startKeyBldr.build());
  }

  /**
   * Spins through the {@link Cell}s and determines which regions the data
   * will be loaded into. Searching the regions is done via a binary search. The
   * region start key should be provided by the caller to cut down on calls to
   * HMaster to get those start keys.
   */
  public static class DetermineAffectedRegionsFn<C extends Cell> extends DoFn<Pair<C, Void>, ByteBuffer> {

    private final Set<Cell> startKeysToEmit = new HashSet<>();
    List<byte[]> startKeys;
    TotalOrderPartitioner.Node partitions;
    List<Cell> regionStartKeys = Lists.newArrayList();

    public DetermineAffectedRegionsFn(List<byte[]> startKeys) {
      this.startKeys = startKeys;
    }

    @Override
    public void initialize() {
      for (byte[] startKey : startKeys.subList(1, startKeys.size())) {
        Cell cell = KeyValueUtil.createFirstOnRow(startKey);
        regionStartKeys.add(cell);
      }

      partitions = new TotalOrderPartitioner.BinarySearchNode<>(regionStartKeys.toArray(new Cell[regionStartKeys.size()]),
              new KeyValue.KVComparator());
    }

    @Override
    public void process(Pair<C, Void> input, Emitter<ByteBuffer> emitter) {
      int position = partitions.findPartition(new KeyValue(input.first().getFamilyArray()));
      // if the position is after the last key, use the last start key
      // as the split for this key, since it should fall into that region
      if (position >= regionStartKeys.size() && regionStartKeys.size() > 1) {
        position = regionStartKeys.size() - 1;
      }

      Cell foundCell = regionStartKeys.get(position);

      if (!startKeysToEmit.contains(foundCell)) {
        startKeysToEmit.add(foundCell);
        emitter.emit(ByteBuffer.wrap(CellUtil.cloneRow(foundCell)));
      }
    }
  }

  private static void writePartitionInfo(
      Configuration conf,
      Path path,
      List<KeyValue> splitPoints) throws IOException {
    LOG.info("Writing {} split points to {}", splitPoints.size(), path);
    SequenceFile.Writer writer = SequenceFile.createWriter(
        path.getFileSystem(conf),
        conf,
        path,
        NullWritable.class,
        BytesWritable.class);
    for (KeyValue key : splitPoints) {
      writer.append(NullWritable.get(), HBaseTypes.keyValueToBytes(key));
    }
    writer.close();
  }

  private static void writeRegionLocationTable(Configuration conf, Path outputPath,
      RegionLocationTable regionLocationTable) throws IOException {
    LOG.info("Writing region location table for {} to {}", regionLocationTable.getTableName(), outputPath);
    try (FSDataOutputStream fsDataOutputStream = outputPath.getFileSystem(conf).create(outputPath)) {
      regionLocationTable.serialize(fsDataOutputStream);
    }
  }

  private static Result doCombineIntoRow(List<KeyValue> kvs, int versions) {
    // shortcut for the common case
    if (kvs.isEmpty()) {
      return null;
    }
    if (kvs.size() == 1 && kvs.get(0).getTypeByte() == KeyValue.Type.Put.getCode()) {
      return Result.create(Collections.<Cell>singletonList(kvs.get(0)));
    }

    kvs = maybeDeleteFamily(kvs);

    // In-place sort KeyValues by family, qualifier and then timestamp reversely (whenever ties, deletes appear first).
    Collections.sort(kvs, KEY_VALUE_COMPARATOR);

    List<Cell> results = Lists.newArrayListWithCapacity(kvs.size());
    for (int i = 0, j; i < kvs.size(); i = j) {
      j = i + 1;
      while (j < kvs.size() && hasSameFamilyAndQualifier(kvs.get(i), kvs.get(j))) {
        j++;
      }
      results.addAll(getLatestKeyValuesOfColumn(kvs.subList(i, j), versions));
    }
    if (results.isEmpty()) {
      return null;
    }
    return Result.create(results);
  }

  /**
   * In-place removes any {@link KeyValue}s whose timestamp is less than or equal to the
   * delete family timestamp. Also removes the delete family {@code KeyValue}s.
   */
  private static List<KeyValue> maybeDeleteFamily(List<KeyValue> kvs) {
    long deleteFamilyCut = -1;
    for (KeyValue kv : kvs) {
      if (kv.getTypeByte() == KeyValue.Type.DeleteFamily.getCode()) {
        deleteFamilyCut = Math.max(deleteFamilyCut, kv.getTimestamp());
      }
    }
    if (deleteFamilyCut == 0) {
      return kvs;
    }
    List<KeyValue> results = Lists.newArrayList();
    for (KeyValue kv : kvs) {
      if (kv.getTypeByte() == KeyValue.Type.DeleteFamily.getCode()) {
        continue;
      }
      if (kv.getTimestamp() <= deleteFamilyCut) {
        continue;
      }
      results.add(kv);
    }
    return results;
  }

  private static boolean hasSameFamilyAndQualifier(KeyValue l, KeyValue r) {
    return Bytes.equals(
        l.getBuffer(), l.getFamilyOffset(), l.getFamilyLength(),
        r.getBuffer(), r.getFamilyOffset(), r.getFamilyLength())
        && Bytes.equals(
        l.getBuffer(), l.getQualifierOffset(), l.getQualifierLength(),
        r.getBuffer(), r.getQualifierOffset(), r.getQualifierLength());
  }

  /**
   * Goes over the given {@link KeyValue}s and remove {@code Delete}s and {@code DeleteColumn}s.
   *
   * @param kvs {@code KeyValue}s that of same row and column and sorted by timestamps in
   *            descending order
   * @param versions the number of versions to keep
   * @return the resulting {@code KeyValue}s that contains only {@code Put}s
   */
  private static List<KeyValue> getLatestKeyValuesOfColumn(List<KeyValue> kvs, int versions) {
    if (kvs.isEmpty()) {
      return kvs;
    }
    if (kvs.get(0).getTypeByte() == KeyValue.Type.Put.getCode()) {
      return kvs; // shortcut for the common case
    }

    List<KeyValue> results = Lists.newArrayListWithCapacity(versions);
    long previousDeleteTimestamp = -1;
    for (KeyValue kv : kvs) {
      if (results.size() >= versions) {
        break;
      }
      if (kv.getTypeByte() == KeyValue.Type.DeleteColumn.getCode()) {
        break;
      } else if (kv.getTypeByte() == KeyValue.Type.Put.getCode()) {
        if (kv.getTimestamp() != previousDeleteTimestamp) {
          results.add(kv);
        }
      } else if (kv.getTypeByte() == KeyValue.Type.Delete.getCode()) {
        previousDeleteTimestamp = kv.getTimestamp();
      } else {
        throw new AssertionError("Unexpected KeyValue type: " + kv.getTypeByte());
      }
    }
    return results;
  }
}
