/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.crunch.lib;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.io.BinaryData;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.GroupingOptions;
import com.cloudera.crunch.GroupingOptions.Builder;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.Tuple3;
import com.cloudera.crunch.Tuple4;
import com.cloudera.crunch.TupleN;
import com.cloudera.crunch.types.PTableType;
import com.cloudera.crunch.types.PType;
import com.cloudera.crunch.types.PTypeFamily;
import com.cloudera.crunch.types.avro.AvroType;
import com.cloudera.crunch.types.avro.AvroTypeFamily;
import com.cloudera.crunch.types.writable.TupleWritable;
import com.cloudera.crunch.types.writable.WritableTypeFamily;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Utilities for sorting {@code PCollection} instances.
 */
public class Sort {
  
  public enum Order {
    ASCENDING, DESCENDING, IGNORE
  }
  
  /**
   * To sort by column 2 ascending then column 1 descending, you would use:
   * <code>
   * sortPairs(coll, by(2, ASCENDING), by(1, DESCENDING))
   * </code>
   * Column numbering is 1-based.
   */
  public static class ColumnOrder {
    int column;
    Order order;
    public ColumnOrder(int column, Order order) {
      this.column = column;
      this.order = order;
    }
    public static ColumnOrder by(int column, Order order) {
      return new ColumnOrder(column, order);
    }
    
    @Override
    public String toString() {
      return"ColumnOrder: column:" + column + ", Order: " + order;
    }
  }
  
  /**
   * Sorts the {@link PCollection} using the natural ordering of its elements.
   * 
   * @return a {@link PCollection} representing the sorted collection.
   */
  public static <T> PCollection<T> sort(PCollection<T> collection) {
    return sort(collection, Order.ASCENDING);
  }
  
  /**
   * Sorts the {@link PCollection} using the natural ordering of its elements
   * in the order specified.
   * 
   * @return a {@link PCollection} representing the sorted collection.
   */
  public static <T> PCollection<T> sort(PCollection<T> collection, Order order) {
    PTypeFamily tf = collection.getTypeFamily();
    PTableType<T, Void> type = tf.tableOf(collection.getPType(), tf.nulls());
    Configuration conf = collection.getPipeline().getConfiguration();
    GroupingOptions options = buildGroupingOptions(conf, tf,
        collection.getPType(), order);
    PTable<T, Void> pt =
      collection.parallelDo("sort-pre", new DoFn<T, Pair<T, Void>>() {
        @Override
        public void process(T input,
            Emitter<Pair<T, Void>> emitter) {
          emitter.emit(Pair.of(input, (Void) null));
        }
      }, type);
    PTable<T, Void> sortedPt = pt.groupByKey(options).ungroup();
    return sortedPt.parallelDo("sort-post", new DoFn<Pair<T, Void>, T>() {
      @Override
      public void process(Pair<T, Void> input, Emitter<T> emitter) {
        emitter.emit(input.first());
      }
    }, collection.getPType());
  }
  

  /**
   * Sorts the {@link PTable} using the natural ordering of its keys.
   * 
   * @return a {@link PTable} representing the sorted table.
   */
  public static <K, V> PTable<K, V> sort(PTable<K, V> table) {
    return sort(table, Order.ASCENDING);
  }

  /**
   * Sorts the {@link PTable} using the natural ordering of its keys
   * in the order specified.
   * 
   * @return a {@link PTable} representing the sorted collection.
   */
  public static <K, V> PTable<K, V> sort(PTable<K, V> table, Order key) {
    PTypeFamily tf = table.getTypeFamily();
    Configuration conf = table.getPipeline().getConfiguration();
    GroupingOptions options = buildGroupingOptions(conf, tf, table.getKeyType(), key);
    return table.groupByKey(options).ungroup();
  }
  
  /**
   * Sorts the {@link PCollection} of {@link Pair}s using the specified column
   * ordering.
   * 
   * @return a {@link PCollection} representing the sorted collection.
   */
  public static <U, V> PCollection<Pair<U, V>> sortPairs(
      PCollection<Pair<U, V>> collection, ColumnOrder... columnOrders) {
    // put U and V into a pair/tuple in the key so we can do grouping and sorting
    PTypeFamily tf = collection.getTypeFamily();
    PType<Pair<U, V>> pType = collection.getPType();
    @SuppressWarnings("unchecked")
    PTableType<Pair<U, V>, Void> type = tf.tableOf(
        tf.pairs(pType.getSubTypes().get(0), pType.getSubTypes().get(1)),
        tf.nulls());
    PTable<Pair<U, V>, Void> pt =
      collection.parallelDo(new DoFn<Pair<U, V>, Pair<Pair<U, V>, Void>>() {
        @Override
        public void process(Pair<U, V> input,
            Emitter<Pair<Pair<U, V>, Void>> emitter) {
          emitter.emit(Pair.of(input, (Void) null));
        }
      }, type);
    Configuration conf = collection.getPipeline().getConfiguration();
    GroupingOptions options = buildGroupingOptions(conf, tf, pType, columnOrders);
    PTable<Pair<U, V>, Void> sortedPt = pt.groupByKey(options).ungroup();
    return sortedPt.parallelDo(new DoFn<Pair<Pair<U, V>,Void>, Pair<U, V>>() {
      @Override
      public void process(Pair<Pair<U, V>, Void> input,
          Emitter<Pair<U, V>> emitter) {
        emitter.emit(input.first());
      }
    }, collection.getPType());
  }

  /**
   * Sorts the {@link PCollection} of {@link Tuple3}s using the specified column
   * ordering.
   * 
   * @return a {@link PCollection} representing the sorted collection.
   */
  public static <V1, V2, V3> PCollection<Tuple3<V1, V2, V3>> sortTriples(
      PCollection<Tuple3<V1, V2, V3>> collection, ColumnOrder... columnOrders) {
    PTypeFamily tf = collection.getTypeFamily();
    PType<Tuple3<V1, V2, V3>> pType = collection.getPType();
    @SuppressWarnings("unchecked")
    PTableType<Tuple3<V1, V2, V3>, Void> type = tf.tableOf(
        tf.triples(pType.getSubTypes().get(0), pType.getSubTypes().get(1), pType.getSubTypes().get(2)),
        tf.nulls());
    PTable<Tuple3<V1, V2, V3>, Void> pt =
      collection.parallelDo(new DoFn<Tuple3<V1, V2, V3>, Pair<Tuple3<V1, V2, V3>, Void>>() {
        @Override
        public void process(Tuple3<V1, V2, V3> input,
            Emitter<Pair<Tuple3<V1, V2, V3>, Void>> emitter) {
          emitter.emit(Pair.of(input, (Void) null));
        }
      }, type);
    Configuration conf = collection.getPipeline().getConfiguration();
    GroupingOptions options = buildGroupingOptions(conf, tf, pType, columnOrders);
    PTable<Tuple3<V1, V2, V3>, Void> sortedPt = pt.groupByKey(options).ungroup();
    return sortedPt.parallelDo(new DoFn<Pair<Tuple3<V1, V2, V3>,Void>, Tuple3<V1, V2, V3>>() {
      @Override
      public void process(Pair<Tuple3<V1, V2, V3>, Void> input,
          Emitter<Tuple3<V1, V2, V3>> emitter) {
        emitter.emit(input.first());
      }
    }, collection.getPType());
  }

  /**
   * Sorts the {@link PCollection} of {@link Tuple4}s using the specified column
   * ordering.
   * 
   * @return a {@link PCollection} representing the sorted collection.
   */
  public static <V1, V2, V3, V4> PCollection<Tuple4<V1, V2, V3, V4>> sortQuads(
      PCollection<Tuple4<V1, V2, V3, V4>> collection, ColumnOrder... columnOrders) {
    PTypeFamily tf = collection.getTypeFamily();
    PType<Tuple4<V1, V2, V3, V4>> pType = collection.getPType();
    @SuppressWarnings("unchecked")
    PTableType<Tuple4<V1, V2, V3, V4>, Void> type = tf.tableOf(
        tf.quads(pType.getSubTypes().get(0), pType.getSubTypes().get(1), pType.getSubTypes().get(2),  pType.getSubTypes().get(3)),
        tf.nulls());
    PTable<Tuple4<V1, V2, V3, V4>, Void> pt =
      collection.parallelDo(new DoFn<Tuple4<V1, V2, V3, V4>, Pair<Tuple4<V1, V2, V3, V4>, Void>>() {
        @Override
        public void process(Tuple4<V1, V2, V3, V4> input,
            Emitter<Pair<Tuple4<V1, V2, V3, V4>, Void>> emitter) {
          emitter.emit(Pair.of(input, (Void) null));
        }
      }, type);
    Configuration conf = collection.getPipeline().getConfiguration();
    GroupingOptions options = buildGroupingOptions(conf, tf, pType, columnOrders);
    PTable<Tuple4<V1, V2, V3, V4>, Void> sortedPt = pt.groupByKey(options).ungroup();
    return sortedPt.parallelDo(new DoFn<Pair<Tuple4<V1, V2, V3, V4>,Void>, Tuple4<V1, V2, V3, V4>>() {
      @Override
      public void process(Pair<Tuple4<V1, V2, V3, V4>, Void> input,
          Emitter<Tuple4<V1, V2, V3, V4>> emitter) {
        emitter.emit(input.first());
      }
    }, collection.getPType());
  }

  /**
   * Sorts the {@link PCollection} of {@link TupleN}s using the specified column
   * ordering.
   * 
   * @return a {@link PCollection} representing the sorted collection.
   */
  public static PCollection<TupleN> sortTuples(PCollection<TupleN> collection,
      ColumnOrder... columnOrders) {
    PTypeFamily tf = collection.getTypeFamily();
    PType<TupleN> pType = collection.getPType();
    PTableType<TupleN, Void> type = tf.tableOf(
        tf.tuples(pType.getSubTypes().toArray(new PType[0])),
        tf.nulls());
    PTable<TupleN, Void> pt =
      collection.parallelDo(new DoFn<TupleN, Pair<TupleN, Void>>() {
        @Override
        public void process(TupleN input,
            Emitter<Pair<TupleN, Void>> emitter) {
          emitter.emit(Pair.of(input, (Void) null));
        }
      }, type);
    Configuration conf = collection.getPipeline().getConfiguration();
    GroupingOptions options = buildGroupingOptions(conf, tf, pType, columnOrders);
    PTable<TupleN, Void> sortedPt = pt.groupByKey(options).ungroup();
    return sortedPt.parallelDo(new DoFn<Pair<TupleN,Void>, TupleN>() {
      @Override
      public void process(Pair<TupleN, Void> input,
          Emitter<TupleN> emitter) {
        emitter.emit(input.first());
      }
    }, collection.getPType());
  }
  
  // TODO: move to type family?
  private static GroupingOptions buildGroupingOptions(Configuration conf,
      PTypeFamily tf, PType ptype, Order order) {
    Builder builder = GroupingOptions.builder();
    if (order == Order.DESCENDING) {
      if (tf == WritableTypeFamily.getInstance()) {
        builder.sortComparatorClass(ReverseWritableComparator.class);
      } else if (tf == AvroTypeFamily.getInstance()) {
        AvroType avroType = (AvroType) ptype;
        Schema schema = avroType.getSchema();
        conf.set("crunch.schema", schema.toString());
        builder.sortComparatorClass(ReverseAvroComparator.class);
      } else {
        throw new RuntimeException("Unrecognized type family: " + tf);
      }
    }
    return builder.build();
  }
  
  private static GroupingOptions buildGroupingOptions(Configuration conf,
      PTypeFamily tf, PType ptype, ColumnOrder[] columnOrders) {
    Builder builder = GroupingOptions.builder();
    if (tf == WritableTypeFamily.getInstance()) {
      TupleWritableComparator.configureOrdering(conf, columnOrders);
      builder.sortComparatorClass(TupleWritableComparator.class);
    } else if (tf == AvroTypeFamily.getInstance()) {
      TupleAvroComparator.configureOrdering(conf, columnOrders, ptype);
      builder.sortComparatorClass(TupleAvroComparator.class);
    } else {
      throw new RuntimeException("Unrecognized type family: " + tf);
    }
    return builder.build();
  }
  
  static class ReverseWritableComparator<T> extends Configured implements RawComparator<T> {
    
    RawComparator<T> comparator;
    
    @SuppressWarnings("unchecked")
    @Override
    public void setConf(Configuration conf) {
      super.setConf(conf);
      if (conf != null) {
        JobConf jobConf = new JobConf(conf);
        comparator = WritableComparator.get(
            jobConf.getMapOutputKeyClass().asSubclass(WritableComparable.class));
      }
    }

    @Override
    public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4,
        int arg5) {
      return -comparator.compare(arg0, arg1, arg2, arg3, arg4, arg5);
    }

    @Override
    public int compare(T o1, T o2) {
      return -comparator.compare(o1, o2);
    }

  }
  
  static class ReverseAvroComparator<T> extends Configured implements RawComparator<T> {

    Schema schema;
    
    @Override
    public void setConf(Configuration conf) {
      super.setConf(conf);
      if (conf != null) {
        schema = Schema.parse(conf.get("crunch.schema"));
      }
    }

    @Override
    public int compare(T o1, T o2) {
      return -ReflectData.get().compare(o1, o2, schema);
    }

    @Override
    public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4,
        int arg5) {
      return -BinaryData.compare(arg0, arg1, arg2, arg3, arg4, arg5, schema);
    }
    
  }
  
  static class TupleWritableComparator extends WritableComparator implements Configurable {
    
    private static final String CRUNCH_ORDERING_PROPERTY = "crunch.ordering";
    
    Configuration conf;
    ColumnOrder[] columnOrders;
    
    public TupleWritableComparator() {
      super(TupleWritable.class, true);
    }
    
    public static void configureOrdering(Configuration conf, Order... orders) {
      conf.set(CRUNCH_ORDERING_PROPERTY, Joiner.on(",").join(
        Iterables.transform(Arrays.asList(orders),
          new Function<Order, String>() {
        @Override
        public String apply(Order o) {
          return o.name();
        }
      })));
    }

    
    public static void configureOrdering(Configuration conf, ColumnOrder... columnOrders) {
      conf.set(CRUNCH_ORDERING_PROPERTY, Joiner.on(",").join(
        Iterables.transform(Arrays.asList(columnOrders),
          new Function<ColumnOrder, String>() {
        @Override
        public String apply(ColumnOrder o) {
          return o.column + ";" + o.order.name();
        }
      })));
    }
    
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      TupleWritable ta = (TupleWritable) a;
      TupleWritable tb = (TupleWritable) b;
      for (int i = 0; i < columnOrders.length; i++) {
        int index = columnOrders[i].column - 1;
        int order = 1;
        if (columnOrders[i].order == Order.ASCENDING) {
          order = 1;
        } else  if (columnOrders[i].order == Order.DESCENDING) {
          order = -1;
        } else { // ignore
          continue;
        }
        if (!ta.has(index) && !tb.has(index)) {
          continue;
        } else if (ta.has(index) && !tb.has(index)) {
          return order;
        } else if (!ta.has(index) && tb.has(index)) {
          return -order;
        } else {
          Writable v1 = ta.get(index);
          Writable v2 = tb.get(index);
          if (v1 != v2 && (v1 != null && !v1.equals(v2))) {
            if (v1 instanceof WritableComparable
                && v2 instanceof WritableComparable) {
              int cmp = ((WritableComparable) v1)
                  .compareTo((WritableComparable) v2);
              if (cmp != 0) {
                return order * cmp;
              }
            } else {
              int cmp = v1.hashCode() - v2.hashCode();
              if (cmp != 0) {
                return order * cmp;
              }
            }
          }
        }
      }
      return 0; // ordering using specified columns found no differences
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
      if (conf != null) {
        String ordering = conf.get(CRUNCH_ORDERING_PROPERTY);
        String[] columnOrderNames = ordering.split(",");
        columnOrders = new ColumnOrder[columnOrderNames.length];
        for (int i = 0; i < columnOrders.length; i++) {
          String[] split = columnOrderNames[i].split(";");
          int column = Integer.parseInt(split[0]);
          Order order = Order.valueOf(split[1]);
          columnOrders[i] = ColumnOrder.by(column, order);
          
        }
      }
    }
  }
  
  static class TupleAvroComparator<T> extends Configured implements RawComparator<T> {

    Schema schema;
    
    @Override
    public void setConf(Configuration conf) {
      super.setConf(conf);
      if (conf != null) {
        schema = Schema.parse(conf.get("crunch.schema"));
      }
    }

    public static void configureOrdering(Configuration conf, ColumnOrder[] columnOrders,
        PType ptype) {
      Schema orderedSchema = createOrderedTupleSchema(ptype, columnOrders);
      conf.set("crunch.schema", orderedSchema.toString());
    }
    
    // TODO: move to Avros
    // TODO: need to re-order columns in map output then switch back in the reduce
    //       this will require more extensive changes in Crunch
    private static Schema createOrderedTupleSchema(PType ptype, ColumnOrder[] orders) {
      // Guarantee each tuple schema has a globally unique name
      String tupleName = "tuple" + UUID.randomUUID().toString().replace('-', 'x');
      Schema schema = Schema.createRecord(tupleName, "", "crunch", false);
      List<Schema.Field> fields = Lists.newArrayList();
      AvroType parentAvroType = (AvroType) ptype;
      Schema parentAvroSchema = parentAvroType.getSchema();
      
      BitSet orderedColumns = new BitSet();
      // First add any fields specified by ColumnOrder
      for (ColumnOrder columnOrder : orders) {
        int index = columnOrder.column - 1;
        AvroType atype = (AvroType) ptype.getSubTypes().get(index);
        Schema fieldSchema = Schema.createUnion(
            ImmutableList.of(atype.getSchema(), Schema.create(Type.NULL)));
        String fieldName = parentAvroSchema.getFields().get(index).name();
        fields.add(new Schema.Field(fieldName, fieldSchema, "", null,
            Schema.Field.Order.valueOf(columnOrder.order.name())));
        orderedColumns.set(index);
      }
      // Then add remaining fields from the ptypes, with no sort order
      for (int i = 0; i < ptype.getSubTypes().size(); i++) {
        if (orderedColumns.get(i)) {
          continue;
        }
        AvroType atype = (AvroType) ptype.getSubTypes().get(i);
        Schema fieldSchema = Schema.createUnion(
            ImmutableList.of(atype.getSchema(), Schema.create(Type.NULL)));
        String fieldName = parentAvroSchema.getFields().get(i).name();
        fields.add(new Schema.Field(fieldName, fieldSchema, "", null,
            Schema.Field.Order.IGNORE));
      }
      schema.setFields(fields);
      return schema;
    }

    @Override
    public int compare(T o1, T o2) {
      return ReflectData.get().compare(o1, o2, schema);
    }

    @Override
    public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4,
        int arg5) {
      return BinaryData.compare(arg0, arg1, arg2, arg3, arg4, arg5, schema);
    }
    
  }
}
