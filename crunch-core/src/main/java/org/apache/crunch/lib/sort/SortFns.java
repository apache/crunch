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
package org.apache.crunch.lib.sort;

import java.util.List;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.crunch.MapFn;
import org.apache.crunch.Tuple;
import org.apache.crunch.lib.Sort.ColumnOrder;
import org.apache.crunch.lib.Sort.Order;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.TupleFactory;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.avro.Avros;

import com.google.common.collect.Lists;

/**
 * A set of {@code DoFn}s that are used by Crunch's {@code Sort} library.
 */
public class SortFns {

  /**
   * Extracts a single indexed key from a {@code Tuple} instance.
   */
  public static class SingleKeyFn<V extends Tuple, K> extends MapFn<V, K> {
    private final int index;
    
    public SingleKeyFn(int index) {
      this.index = index;
    }

    @Override
    public K map(V input) {
      return (K) input.get(index);
    }
  }

  /**
   * Extracts a composite key from a {@code Tuple} instance.
   */
  public static class TupleKeyFn<V extends Tuple, K extends Tuple> extends MapFn<V, K> {
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
  
  /**
   * Pulls a composite set of keys from an Avro {@code GenericRecord} instance.
   */
  public static class AvroGenericFn<V extends Tuple> extends MapFn<V, GenericRecord> {

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
  
  /**
   * Constructs an Avro schema for the given {@code PType<S>} that respects the given column
   * orderings.
   */
  public static <S> Schema createOrderedTupleSchema(PType<S> ptype, ColumnOrder[] orders) {
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
      Schema.Field.Order order = columnOrder.order() == Order.DESCENDING ? Schema.Field.Order.DESCENDING :
        Schema.Field.Order.ASCENDING;
      fields.add(new Schema.Field(fieldName, fieldSchema, "", null, order));
    }
    schema.setFields(fields);
    return schema;
  }

  /**
   * Utility class for encapsulating key extraction logic and serialization information about
   * key extraction.
   */
  public static class KeyExtraction<V extends Tuple> {

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
        cols[i] = columnOrder[i].column() - 1;
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
        TupleFactory tf;
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
}
