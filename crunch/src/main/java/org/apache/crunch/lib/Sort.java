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

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryData;
import org.apache.avro.reflect.ReflectData;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.GroupingOptions.Builder;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Tuple;
import org.apache.crunch.Tuple3;
import org.apache.crunch.Tuple4;
import org.apache.crunch.TupleN;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.TupleFactory;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.TupleWritable;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Utilities for sorting {@code PCollection} instances.
 */
public class Sort {

  public enum Order {
    ASCENDING,
    DESCENDING,
    IGNORE
  }

  /**
   * To sort by column 2 ascending then column 1 descending, you would use:
   * <code>
   * sortPairs(coll, by(2, ASCENDING), by(1, DESCENDING))
   * </code> Column numbering is 1-based.
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
      return "ColumnOrder: column:" + column + ", Order: " + order;
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
   * Sorts the {@link PCollection} using the natural ordering of its elements in
   * the order specified.
   * 
   * @return a {@link PCollection} representing the sorted collection.
   */
  public static <T> PCollection<T> sort(PCollection<T> collection, Order order) {
    PTypeFamily tf = collection.getTypeFamily();
    PTableType<T, Void> type = tf.tableOf(collection.getPType(), tf.nulls());
    Configuration conf = collection.getPipeline().getConfiguration();
    GroupingOptions options = buildGroupingOptions(conf, tf, collection.getPType(), order);
    PTable<T, Void> pt = collection.parallelDo("sort-pre", new DoFn<T, Pair<T, Void>>() {
      @Override
      public void process(T input, Emitter<Pair<T, Void>> emitter) {
        emitter.emit(Pair.of(input, (Void) null));
      }
    }, type);
    return pt.groupByKey(options).ungroup().keys();
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
   * Sorts the {@link PTable} using the natural ordering of its keys in the
   * order specified.
   * 
   * @return a {@link PTable} representing the sorted collection.
   */
  public static <K, V> PTable<K, V> sort(PTable<K, V> table, Order key) {
    PTypeFamily tf = table.getTypeFamily();
    Configuration conf = table.getPipeline().getConfiguration();
    GroupingOptions options = buildGroupingOptions(conf, tf, table.getKeyType(), key);
    return table.groupByKey(options).ungroup();
  }

  static class SingleKeyFn<V extends Tuple, K> extends MapFn<V, K> {
    private final int index;
    
    public SingleKeyFn(int index) {
      this.index = index;
    }

    @Override
    public K map(V input) {
      return (K) input.get(index);
    }
  }
  
  static class TupleKeyFn<V extends Tuple, K extends Tuple> extends MapFn<V, K> {
    private final int[] indices;
    private final TupleFactory tupleFactory;
    
    public TupleKeyFn(int[] indices, TupleFactory tupleFactory) {
      this.indices = indices;
      this.tupleFactory = tupleFactory;
    }
    
    @Override
    public K map(V input) {
      Object[] values = new Object[indices.length];
      for (int i = 0; i < indices.length; i++) {
        values[i] = input.get(indices[i]);
      }
      return (K) tupleFactory.makeTuple(values);
    }
  }
  
  static class AvroGenericFn<V extends Tuple> extends MapFn<V, GenericRecord> {

    private final int[] indices;
    private final String schemaJson;
    private transient Schema schema;
    
    public AvroGenericFn(int[] indices, Schema schema) {
      this.indices = indices;
      this.schemaJson = schema.toString();
    }
    
    @Override
    public void initialize() {
      this.schema = (new Schema.Parser()).parse(schemaJson);
    }
    
    @Override
    public GenericRecord map(V input) {
      GenericRecord rec = new GenericData.Record(schema);
      for (int i = 0; i < indices.length; i++) {
        rec.put(i, input.get(indices[i]));
      }
      return rec;
    }
  }
  
  static <S> Schema createOrderedTupleSchema(PType<S> ptype, ColumnOrder[] orders) {
    // Guarantee each tuple schema has a globally unique name
    String tupleName = "tuple" + UUID.randomUUID().toString().replace('-', 'x');
    Schema schema = Schema.createRecord(tupleName, "", "crunch", false);
    List<Schema.Field> fields = Lists.newArrayList();
    AvroType<S> parentAvroType = (AvroType<S>) ptype;
    Schema parentAvroSchema = parentAvroType.getSchema();

    for (int index = 0; index < orders.length; index++) {
      ColumnOrder columnOrder = orders[index];
      AvroType<?> atype = (AvroType<?>) ptype.getSubTypes().get(index);
      Schema fieldSchema = atype.getSchema();
      String fieldName = parentAvroSchema.getFields().get(index).name();
      // Note: avro sorting of strings is inverted relative to how sorting works for WritableComparable
      // Text instances: making this consistent
      Schema.Field.Order order = columnOrder.order == Order.DESCENDING ? Schema.Field.Order.DESCENDING :
        Schema.Field.Order.ASCENDING;
      fields.add(new Schema.Field(fieldName, fieldSchema, "", null, order));
    }
    schema.setFields(fields);
    return schema;
  }

  static class KeyExtraction<V extends Tuple> {

    private PType<V> ptype;
    private final ColumnOrder[] columnOrder;
    private final int[] cols;
    
    private MapFn<V, Object> byFn;
    private PType<Object> keyPType;
    
    public KeyExtraction(PType<V> ptype, ColumnOrder[] columnOrder) {
      this.ptype = ptype;
      this.columnOrder = columnOrder;
      this.cols = new int[columnOrder.length];
      for (int i = 0; i < columnOrder.length; i++) {
        cols[i] = columnOrder[i].column - 1;
      }
      init();
    }
    
    private void init() {
      List<PType> pt = ptype.getSubTypes();
      PTypeFamily ptf = ptype.getFamily();
      if (cols.length == 1) {
        byFn = new SingleKeyFn(cols[0]);
        keyPType = pt.get(cols[0]);
      } else {
        TupleFactory tf = null;
        switch (cols.length) {
        case 2:
          tf = TupleFactory.PAIR;
          keyPType = ptf.pairs(pt.get(cols[0]), pt.get(cols[1]));
          break;
        case 3:
          tf = TupleFactory.TUPLE3;
          keyPType = ptf.triples(pt.get(cols[0]), pt.get(cols[1]), pt.get(cols[2]));
          break;
        case 4:
          tf = TupleFactory.TUPLE4;
          keyPType = ptf.quads(pt.get(cols[0]), pt.get(cols[1]), pt.get(cols[2]), pt.get(cols[3]));
          break;
        default:
          PType[] pts = new PType[cols.length];
          for (int i = 0; i < pts.length; i++) {
            pts[i] = pt.get(cols[i]);
          }
          tf = TupleFactory.TUPLEN;
          keyPType = (PType<Object>) (PType<?>) ptf.tuples(pts);
        }
        
        if (ptf == AvroTypeFamily.getInstance()) {
          Schema s = createOrderedTupleSchema(keyPType, columnOrder);
          keyPType = (PType<Object>) (PType<?>) Avros.generics(s);
          byFn = new AvroGenericFn(cols, s);
        } else {
          byFn = new TupleKeyFn(cols, tf);
        }
      }
      
    }

    public MapFn<V, Object> getByFn() {
      return byFn;
    }
    
    public PType<Object> getKeyType() {
      return keyPType;
    }
  }
  
  /**
   * Sorts the {@link PCollection} of {@link Pair}s using the specified column
   * ordering.
   * 
   * @return a {@link PCollection} representing the sorted collection.
   */
  public static <U, V> PCollection<Pair<U, V>> sortPairs(PCollection<Pair<U, V>> collection,
      ColumnOrder... columnOrders) {
    return sortTuples(collection, columnOrders);
  }

  /**
   * Sorts the {@link PCollection} of {@link Tuple3}s using the specified column
   * ordering.
   * 
   * @return a {@link PCollection} representing the sorted collection.
   */
  public static <V1, V2, V3> PCollection<Tuple3<V1, V2, V3>> sortTriples(PCollection<Tuple3<V1, V2, V3>> collection,
      ColumnOrder... columnOrders) {
    return sortTuples(collection, columnOrders);
  }

  /**
   * Sorts the {@link PCollection} of {@link Tuple4}s using the specified column
   * ordering.
   * 
   * @return a {@link PCollection} representing the sorted collection.
   */
  public static <V1, V2, V3, V4> PCollection<Tuple4<V1, V2, V3, V4>> sortQuads(
      PCollection<Tuple4<V1, V2, V3, V4>> collection, ColumnOrder... columnOrders) {
    return sortTuples(collection, columnOrders);
  }

  /**
   * Sorts the {@link PCollection} of {@link TupleN}s using the specified column
   * ordering.
   * 
   * @return a {@link PCollection} representing the sorted collection.
   */
  public static <T extends Tuple> PCollection<T> sortTuples(PCollection<T> collection, ColumnOrder... columnOrders) {
    PTypeFamily tf = collection.getTypeFamily();
    PType<T> pType = collection.getPType();
    KeyExtraction<T> ke = new KeyExtraction<T>(pType, columnOrders);
    PTable<Object, T> pt = collection.by(ke.getByFn(), ke.getKeyType());
    Configuration conf = collection.getPipeline().getConfiguration();
    GroupingOptions options = buildGroupingOptions(conf, tf, ke.getKeyType(), pType, columnOrders);
    return pt.groupByKey(options).ungroup().values();
  }

  // TODO: move to type family?
  private static <T> GroupingOptions buildGroupingOptions(Configuration conf, PTypeFamily tf, PType<T> ptype,
      Order order) {
    Builder builder = GroupingOptions.builder();
    if (order == Order.DESCENDING) {
      if (tf == WritableTypeFamily.getInstance()) {
        builder.sortComparatorClass(ReverseWritableComparator.class);
      } else if (tf == AvroTypeFamily.getInstance()) {
        AvroType<T> avroType = (AvroType<T>) ptype;
        Schema schema = avroType.getSchema();
        conf.set("crunch.schema", schema.toString());
        builder.sortComparatorClass(ReverseAvroComparator.class);
      } else {
        throw new RuntimeException("Unrecognized type family: " + tf);
      }
    }
    // TODO:CRUNCH-23: Intermediate Fix for release 1. More elaborate fix is
    // required check JIRA for details.
    builder.numReducers(1);
    return builder.build();
  }

  private static <T> GroupingOptions buildGroupingOptions(Configuration conf, PTypeFamily tf, PType<T> keyType,
      PType<?> valueType, ColumnOrder[] columnOrders) {
    Builder builder = GroupingOptions.builder();
    if (tf == WritableTypeFamily.getInstance()) {
      if (columnOrders.length == 1 && columnOrders[0].order == Order.DESCENDING) {
        builder.sortComparatorClass(ReverseWritableComparator.class);
      } else {
        TupleWritableComparator.configureOrdering(conf, columnOrders);
        builder.sortComparatorClass(TupleWritableComparator.class);
      }
    } else if (tf == AvroTypeFamily.getInstance()) {
      if (columnOrders.length == 1 && columnOrders[0].order == Order.DESCENDING) {
        AvroType<T> avroType = (AvroType<T>) keyType;
        Schema schema = avroType.getSchema();
        conf.set("crunch.schema", schema.toString());
        builder.sortComparatorClass(ReverseAvroComparator.class);
      }
    } else {
      throw new RuntimeException("Unrecognized type family: " + tf);
    }
    // TODO:CRUNCH-23: Intermediate Fix for release 1. More elaborate fix is
    // required check JIRA for details.
    builder.numReducers(1);
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
        comparator = WritableComparator.get(jobConf.getMapOutputKeyClass().asSubclass(WritableComparable.class));
      }
    }

    @Override
    public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5) {
      return -comparator.compare(arg0, arg1, arg2, arg3, arg4, arg5);
    }

    @Override
    public int compare(T o1, T o2) {
      return -comparator.compare(o1, o2);
    }

  }

  static class ReverseAvroComparator<T> extends Configured implements RawComparator<T> {

    private Schema schema;

    @Override
    public void setConf(Configuration conf) {
      super.setConf(conf);
      if (conf != null) {
        schema = (new Schema.Parser()).parse(conf.get("crunch.schema"));
      }
    }

    @Override
    public int compare(T o1, T o2) {
      return -ReflectData.get().compare(o1, o2, schema);
    }

    @Override
    public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5) {
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
      conf.set(CRUNCH_ORDERING_PROPERTY,
          Joiner.on(",").join(Iterables.transform(Arrays.asList(orders), new Function<Order, String>() {
            @Override
            public String apply(Order o) {
              return o.name();
            }
          })));
    }

    public static void configureOrdering(Configuration conf, ColumnOrder... columnOrders) {
      conf.set(CRUNCH_ORDERING_PROPERTY,
          Joiner.on(",").join(Iterables.transform(Arrays.asList(columnOrders), new Function<ColumnOrder, String>() {
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
      for (int index = 0; index < columnOrders.length; index++) {
        int order = 1;
        if (columnOrders[index].order == Order.ASCENDING) {
          order = 1;
        } else if (columnOrders[index].order == Order.DESCENDING) {
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
            if (v1 instanceof WritableComparable && v2 instanceof WritableComparable) {
              int cmp = ((WritableComparable) v1).compareTo((WritableComparable) v2);
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
      return 0; // ordering using specified cols found no differences
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

}
