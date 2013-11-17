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
package org.apache.crunch.lib;

import org.apache.avro.Schema;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.Tuple;
import org.apache.crunch.Tuple3;
import org.apache.crunch.Tuple4;
import org.apache.crunch.TupleN;
import org.apache.crunch.lib.sort.SortFns;
import org.apache.crunch.lib.sort.TotalOrderPartitioner;
import org.apache.crunch.lib.sort.ReverseAvroComparator;
import org.apache.crunch.lib.sort.ReverseWritableComparator;
import org.apache.crunch.lib.sort.TupleWritableComparator;
import org.apache.crunch.materialize.MaterializableIterable;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.writable.WritableType;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.crunch.util.PartitionUtils;
import org.apache.hadoop.conf.Configuration;

/**
 * Utilities for sorting {@code PCollection} instances.
 */
public class Sort {

  /**
   * For signaling the order in which a sort should be done.
   */
  public enum Order {
    ASCENDING,
    DESCENDING,
    IGNORE
  }

  /**
   * To sort by column 2 ascending then column 1 descending, you would use:
   * {@code
   * sortPairs(coll, by(2, ASCENDING), by(1, DESCENDING))
   * } Column numbering is 1-based.
   */
  public static class ColumnOrder {
    private int column;
    private Order order;

    public ColumnOrder(int column, Order order) {
      this.column = column;
      this.order = order;
    }

    public static ColumnOrder by(int column, Order order) {
      return new ColumnOrder(column, order);
    }

    public int column() {
      return column;
    }
    
    public Order order() {
      return order;
    }
    
    @Override
    public String toString() {
      return "ColumnOrder: column:" + column + ", Order: " + order;
    }
  }

  /**
   * Sorts the {@code PCollection} using the natural ordering of its elements in ascending order.
   * 
   * @return a {@code PCollection} representing the sorted collection.
   */
  public static <T> PCollection<T> sort(PCollection<T> collection) {
    return sort(collection, Order.ASCENDING);
  }

  /**
   * Sorts the {@code PCollection} using the natural order of its elements with the given {@code Order}.
   * 
   * @return a {@code PCollection} representing the sorted collection.
   */
  public static <T> PCollection<T> sort(PCollection<T> collection, Order order) {
    return sort(collection, -1, order);
  }
  
  /**
   * Sorts the {@code PCollection} using the natural ordering of its elements in
   * the order specified using the given number of reducers.
   * 
   * @return a {@code PCollection} representing the sorted collection.
   */
  public static <T> PCollection<T> sort(PCollection<T> collection, int numReducers, Order order) {
    PTypeFamily tf = collection.getTypeFamily();
    PTableType<T, Void> type = tf.tableOf(collection.getPType(), tf.nulls());
    Configuration conf = collection.getPipeline().getConfiguration();
    PTable<T, Void> pt = collection.parallelDo("sort-pre", new DoFn<T, Pair<T, Void>>() {
      @Override
      public void process(T input, Emitter<Pair<T, Void>> emitter) {
        emitter.emit(Pair.of(input, (Void) null));
      }
    }, type);
    GroupingOptions options = buildGroupingOptions(pt, conf, numReducers, order);
    return pt.groupByKey(options).ungroup().keys();
  }

  /**
   * Sorts the {@code PTable} using the natural ordering of its keys in ascending order.
   * 
   * @return a {@code PTable} representing the sorted table.
   */
  public static <K, V> PTable<K, V> sort(PTable<K, V> table) {
    return sort(table, Order.ASCENDING);
  }

  /**
   * Sorts the {@code PTable} using the natural ordering of its keys with the given {@code Order}.
   *
   * @return a {@code PTable} representing the sorted table.
   */
  public static <K, V> PTable<K, V> sort(PTable<K, V> table, Order key) {
    return sort(table, -1, key);
  }
  
  /**
   * Sorts the {@code PTable} using the natural ordering of its keys in the
   * order specified with a client-specified number of reducers.
   * 
   * @return a {@code PTable} representing the sorted collection.
   */
  public static <K, V> PTable<K, V> sort(PTable<K, V> table, int numReducers, Order key) {
    Configuration conf = table.getPipeline().getConfiguration();
    GroupingOptions options = buildGroupingOptions(table, conf, numReducers, key);
    return table.groupByKey(options).ungroup();
  }

  
  /**
   * Sorts the {@code PCollection} of {@code Pair}s using the specified column
   * ordering.
   * 
   * @return a {@code PCollection} representing the sorted collection.
   */
  public static <U, V> PCollection<Pair<U, V>> sortPairs(PCollection<Pair<U, V>> collection,
      ColumnOrder... columnOrders) {
    return sortTuples(collection, columnOrders);
  }

  /**
   * Sorts the {@code PCollection} of {@code Tuple3}s using the specified column
   * ordering.
   * 
   * @return a {@code PCollection} representing the sorted collection.
   */
  public static <V1, V2, V3> PCollection<Tuple3<V1, V2, V3>> sortTriples(PCollection<Tuple3<V1, V2, V3>> collection,
      ColumnOrder... columnOrders) {
    return sortTuples(collection, columnOrders);
  }

  /**
   * Sorts the {@code PCollection} of {@code Tuple4}s using the specified column
   * ordering.
   * 
   * @return a {@code PCollection} representing the sorted collection.
   */
  public static <V1, V2, V3, V4> PCollection<Tuple4<V1, V2, V3, V4>> sortQuads(
      PCollection<Tuple4<V1, V2, V3, V4>> collection, ColumnOrder... columnOrders) {
    return sortTuples(collection, columnOrders);
  }

  /**
   * Sorts the {@code PCollection} of tuples using the specified column ordering.
   *
   * @return a {@code PCollection} representing the sorted collection.
   */
  public static <T extends Tuple> PCollection<T> sortTuples(PCollection<T> collection,
      ColumnOrder... columnOrders) {
    return sortTuples(collection, -1, columnOrders);
  }
  
  /**
   * Sorts the {@code PCollection} of {@link TupleN}s using the specified column
   * ordering and a client-specified number of reducers.
   * 
   * @return a {@code PCollection} representing the sorted collection.
   */
  public static <T extends Tuple> PCollection<T> sortTuples(PCollection<T> collection, int numReducers,
      ColumnOrder... columnOrders) {
    PType<T> pType = collection.getPType();
    SortFns.KeyExtraction<T> ke = new SortFns.KeyExtraction<T>(pType, columnOrders);
    PTable<Object, T> pt = collection.by(ke.getByFn(), ke.getKeyType());
    Configuration conf = collection.getPipeline().getConfiguration();
    GroupingOptions options = buildGroupingOptions(pt, conf, numReducers, columnOrders);
    return pt.groupByKey(options).ungroup().values();
  }

  // TODO: move to type family?
  private static <K, V> GroupingOptions buildGroupingOptions(PTable<K, V> ptable, Configuration conf,
      int numReducers, Order order) {
    PType<K> ptype = ptable.getKeyType();
    PTypeFamily tf = ptable.getTypeFamily();
    GroupingOptions.Builder builder = GroupingOptions.builder();
    if (order == Order.DESCENDING) {
      if (tf == WritableTypeFamily.getInstance()) {
        builder.sortComparatorClass(ReverseWritableComparator.class);
      } else if (tf == AvroTypeFamily.getInstance()) {
        AvroType<K> avroType = (AvroType<K>) ptype;
        Schema schema = avroType.getSchema();
        builder.conf("crunch.schema", schema.toString());
        builder.sortComparatorClass(ReverseAvroComparator.class);
      } else {
        throw new RuntimeException("Unrecognized type family: " + tf);
      }
    } else if (tf == AvroTypeFamily.getInstance()) {
      builder.conf("crunch.schema", ((AvroType<K>) ptype).getSchema().toString());
    }
    builder.requireSortedKeys();
    configureReducers(builder, ptable, conf, numReducers);
    return builder.build();
  }

  private static <K, V> GroupingOptions buildGroupingOptions(PTable<K, V> ptable, Configuration conf,
      int numReducers, ColumnOrder[] columnOrders) {
    PTypeFamily tf = ptable.getTypeFamily();
    PType<K> keyType = ptable.getKeyType();
    GroupingOptions.Builder builder = GroupingOptions.builder();
    if (tf == WritableTypeFamily.getInstance()) {
      if (columnOrders.length == 1 && columnOrders[0].order == Order.DESCENDING) {
        builder.sortComparatorClass(ReverseWritableComparator.class);
      } else {
        WritableType[] wt = new WritableType[columnOrders.length];
        for (int i = 0; i < wt.length; i++) {
          wt[i] = (WritableType) keyType.getSubTypes().get(i);
        }
        TupleWritableComparator.configureOrdering(conf, wt, columnOrders);
        builder.sortComparatorClass(TupleWritableComparator.class);
      }
    } else if (tf == AvroTypeFamily.getInstance()) {
      AvroType<K> avroType = (AvroType<K>) keyType;
      Schema schema = avroType.getSchema();
      builder.conf("crunch.schema", schema.toString());
      if (columnOrders.length == 1 && columnOrders[0].order == Order.DESCENDING) {
        builder.sortComparatorClass(ReverseAvroComparator.class);
      }
    } else {
      throw new RuntimeException("Unrecognized type family: " + tf);
    }
    builder.requireSortedKeys();
    configureReducers(builder, ptable, conf, numReducers);
    return builder.build();
  }

  private static <K, V> void configureReducers(GroupingOptions.Builder builder,
      PTable<K, V> ptable, Configuration conf, int numReducers) {
    if (numReducers <= 0) {
      numReducers = PartitionUtils.getRecommendedPartitions(ptable, conf);
      if (numReducers < 5) {
        // Not worth the overhead, force it to 1
        numReducers = 1;
      }
    }
    builder.numReducers(numReducers);
    if (numReducers > 1) {
      Iterable<K> iter = Sample.reservoirSample(ptable.keys(), numReducers - 1).materialize();
      MaterializableIterable<K> mi = (MaterializableIterable<K>) iter;
      if (mi.isSourceTarget()) {
        builder.sourceTargets((SourceTarget) mi.getSource());
      }
      builder.partitionerClass(TotalOrderPartitioner.class);
      builder.conf(TotalOrderPartitioner.PARTITIONER_PATH, mi.getPath().toString());
      //TODO: distcache handling
    }   
  }

}
