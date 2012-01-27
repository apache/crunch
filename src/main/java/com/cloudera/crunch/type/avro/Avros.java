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
package com.cloudera.crunch.type.avro;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;

import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.Tuple;
import com.cloudera.crunch.Tuple3;
import com.cloudera.crunch.Tuple4;
import com.cloudera.crunch.TupleN;
import com.cloudera.crunch.fn.CompositeMapFn;
import com.cloudera.crunch.type.PType;
import com.cloudera.crunch.type.TupleFactory;
import com.cloudera.crunch.util.PTypes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Defines static methods that are analogous to the methods defined in
 * {@link AvroTypeFamily} for convenient static importing.
 *
 */
public class Avros {

  public static MapFn<Utf8, String> UTF8_TO_STRING = new MapFn<Utf8, String>() {
    @Override
    public String map(Utf8 input) {
      return input.toString();
    }
  };
  
  public static MapFn<String, Utf8> STRING_TO_UTF8 = new MapFn<String, Utf8>() {
    @Override
    public Utf8 map(String input) {
      return new Utf8(input);
    }
  };

  private static final AvroType<String> strings = new AvroType<String>(
      String.class, Schema.create(Schema.Type.STRING), UTF8_TO_STRING, STRING_TO_UTF8);
  private static final AvroType<Void> nulls = create(Void.class, Schema.Type.NULL);
  private static final AvroType<Long> longs = create(Long.class, Schema.Type.LONG);
  private static final AvroType<Integer> ints = create(Integer.class, Schema.Type.INT);
  private static final AvroType<Float> floats = create(Float.class, Schema.Type.FLOAT);
  private static final AvroType<Double> doubles = create(Double.class, Schema.Type.DOUBLE);
  private static final AvroType<Boolean> booleans = create(Boolean.class, Schema.Type.BOOLEAN);
  private static final AvroType<ByteBuffer> bytes = create(ByteBuffer.class, Schema.Type.BYTES);
  
  private static final Map<Class, PType> PRIMITIVES = ImmutableMap.<Class, PType>builder()
      .put(String.class, strings)
      .put(Long.class, longs)
      .put(Integer.class, ints)
      .put(Float.class, floats)
      .put(Double.class, doubles)
      .put(Boolean.class, booleans)
      .put(ByteBuffer.class, bytes)
      .build();
  
  private static final Map<Class, AvroType> EXTENSIONS = Maps.newHashMap();
  
  public static <T> void register(Class<T> clazz, AvroType<T> ptype) {
    EXTENSIONS.put(clazz, ptype);
  }
  
  public static <T> PType<T> getPrimitiveType(Class<T> clazz) {
    return PRIMITIVES.get(clazz);
  }
  
  private static <T> AvroType<T> create(Class<T> clazz, Schema.Type schemaType) {
    return new AvroType<T>(clazz, Schema.create(schemaType));
  }

  public static final AvroType<Void> nulls() {
    return nulls;
  }

  public static final AvroType<String> strings() {
    return strings;
  }

  public static final AvroType<Long> longs() {
    return longs;
  }

  public static final AvroType<Integer> ints() {
    return ints;
  }

  public static final AvroType<Float> floats() {
    return floats;
  }

  public static final AvroType<Double> doubles() {
    return doubles;
  }

  public static final AvroType<Boolean> booleans() {
    return booleans;
  }
  
  public static final AvroType<ByteBuffer> bytes() {
    return bytes;
  }
  
  public static final <T> AvroType<T> records(Class<T> clazz) {
    if (EXTENSIONS.containsKey(clazz)) {
      return (AvroType<T>) EXTENSIONS.get(clazz);
    }
    return containers(clazz);
  }

  public static final AvroType<GenericData.Record> generics(Schema schema) {
	return new AvroType<GenericData.Record>(GenericData.Record.class, schema);
  }
  
  public static final <T> AvroType<T> containers(Class<T> clazz) {
    return new AvroType<T>(clazz, SpecificData.get().getSchema(clazz));
  }
  
  private static class GenericDataArrayToCollection extends MapFn<GenericData.Array, Collection> {
    
    private final MapFn mapFn;
    
    public GenericDataArrayToCollection(MapFn mapFn) {
      this.mapFn = mapFn;
    }
    
    @Override
    public void initialize() {
      this.mapFn.initialize();
    }
    
    @Override
    public Collection map(GenericData.Array input) {
      Collection ret = Lists.newArrayList();
      for (Object in : input) {
        ret.add(mapFn.map(in));
      }
      return ret;
    }
  }
  
  private static class CollectionToGenericDataArray extends MapFn<Collection, GenericData.Array> {
    
    private final MapFn mapFn;
    private final String jsonSchema;
    private transient Schema schema;
    
    public CollectionToGenericDataArray(Schema schema, MapFn mapFn) {
      this.mapFn = mapFn;
      this.jsonSchema = schema.toString();
    }
    
    @Override
    public void initialize() {
      this.mapFn.initialize();
    }
    
    @Override
    public GenericData.Array map(Collection input) {
      if(schema == null) {
        schema = new Schema.Parser().parse(jsonSchema);
      }
      GenericData.Array array = new GenericData.Array(input.size(), schema);
      for (Object in : input) {
        array.add(mapFn.map(in));
      }
      return array;
    }
  }
  
  public static final <T> AvroType<Collection<T>> collections(PType<T> ptype) {
    AvroType<T> avroType = (AvroType<T>) ptype;
    Schema collectionSchema = Schema.createArray(avroType.getSchema());
    GenericDataArrayToCollection input = new GenericDataArrayToCollection(avroType.getInputMapFn());
    CollectionToGenericDataArray output = new CollectionToGenericDataArray(collectionSchema, avroType.getOutputMapFn());
    return new AvroType(Collection.class, collectionSchema, input, output, ptype);
  }

  private static class AvroMapToMap<T> extends MapFn<Map<Utf8, Object>, Map<String, T>> {
	private final MapFn<Object, T> mapFn;
	
	public AvroMapToMap(MapFn<Object, T> mapFn) {
	  this.mapFn = mapFn;
	}
	
	@Override
	public void initialize() {
	  this.mapFn.initialize();
	}
	
	@Override
	public Map<String, T> map(Map<Utf8, Object> input) {
	  Map<String, T> out = Maps.newHashMap();
	  for (Map.Entry<Utf8, Object> e : input.entrySet()) {
		out.put(e.getKey().toString(), mapFn.map(e.getValue()));
	  }
	  return out;
	}
  }
  
  private static class MapToAvroMap<T> extends MapFn<Map<String, T>, Map<Utf8, Object>> {
	private final MapFn<T, Object> mapFn;
	
	public MapToAvroMap(MapFn<T, Object> mapFn) {
	  this.mapFn = mapFn;
	}
	
	@Override
	public void initialize() {
	  this.mapFn.initialize();
	}
	
	@Override
	public Map<Utf8, Object> map(Map<String, T> input) {
	  Map<Utf8, Object> out = Maps.newHashMap();
	  for (Map.Entry<String, T> e : input.entrySet()) {
		out.put(new Utf8(e.getKey()), mapFn.map(e.getValue()));
	  }
	  return out;
	}
  }
  
  public static final <T> AvroType<Map<String, T>> maps(PType<T> ptype) {
	AvroType<T> avroType = (AvroType<T>) ptype;
	Schema mapSchema = Schema.createMap(avroType.getSchema());
	AvroMapToMap<T> inputFn = new AvroMapToMap<T>(avroType.getInputMapFn());
	MapToAvroMap<T> outputFn = new MapToAvroMap<T>(avroType.getOutputMapFn());
	return new AvroType(Map.class, mapSchema, inputFn, outputFn, ptype);
  }
  
  private static class GenericRecordToTuple extends MapFn<GenericRecord, Tuple> {
    private final TupleFactory tupleFactory;
    private final List<MapFn> fns;
    
    private transient Object[] values;
    
    public GenericRecordToTuple(TupleFactory tupleFactory, PType... ptypes) {
      this.tupleFactory = tupleFactory;
      this.fns = Lists.newArrayList();
      for (PType ptype : ptypes) {
        AvroType atype = (AvroType) ptype;
        fns.add(atype.getInputMapFn());
      }
    }
    
    @Override
    public void initialize() {
      for (MapFn fn : fns) {
        fn.initialize();
      }
      this.values = new Object[fns.size()];
      tupleFactory.initialize();
    }

    @Override
    public Tuple map(GenericRecord input) {
      for (int i = 0; i < values.length; i++) {
        Object v = input.get(i);
        if (v == null) {
          values[i] = null;
        } else {
          values[i] = fns.get(i).map(v);
        }
      }
      return tupleFactory.makeTuple(values);
    }
  }
  
  private static class TupleToGenericRecord extends MapFn<Tuple, GenericRecord> {
    private final List<MapFn> fns;
    private final String jsonSchema;
    
    private transient GenericRecord record;
    
    public TupleToGenericRecord(Schema schema, PType... ptypes) {
      this.fns = Lists.newArrayList();
      this.jsonSchema = schema.toString();
      for (PType ptype : ptypes) {
        AvroType atype = (AvroType) ptype;
        fns.add(atype.getOutputMapFn());
      }
    }
    
    @Override
    public void initialize() {
      this.record = new GenericData.Record(new Schema.Parser().parse(jsonSchema));
      for (MapFn fn : fns) {
        fn.initialize();
      }
    }

    @Override
    public GenericRecord map(Tuple input) {
      for (int i = 0; i < input.size(); i++) {
        Object v = input.get(i);
        if (v == null) {
          record.put(i, null);
        } else {
          record.put(i, fns.get(i).map(v));
        }
      }
      return record;
    }
  }
  
  public static final <V1, V2> AvroType<Pair<V1, V2>> pairs(PType<V1> p1, PType<V2> p2) {
    Schema schema = createTupleSchema(p1, p2);
    GenericRecordToTuple input = new GenericRecordToTuple(TupleFactory.PAIR, p1, p2);
    input.initialize();
    TupleToGenericRecord output = new TupleToGenericRecord(schema, p1, p2);
    output.initialize();
    return new AvroType(Pair.class, schema, input, output, p1, p2);
  }

  public static final <V1, V2, V3> AvroType<Tuple3<V1, V2, V3>> triples(PType<V1> p1,
      PType<V2> p2, PType<V3> p3) {
    Schema schema = createTupleSchema(p1, p2, p3);
    return new AvroType(Tuple3.class, schema,
        new GenericRecordToTuple(TupleFactory.TUPLE3, p1, p2, p3),
        new TupleToGenericRecord(schema, p1, p2, p3),
        p1, p2, p3);
  }

  public static final <V1, V2, V3, V4> AvroType<Tuple4<V1, V2, V3, V4>> quads(PType<V1> p1,
      PType<V2> p2, PType<V3> p3, PType<V4> p4) {
    Schema schema = createTupleSchema(p1, p2, p3, p4);
    return new AvroType(Tuple4.class, schema,
        new GenericRecordToTuple(TupleFactory.TUPLE4, p1, p2, p3, p4),
        new TupleToGenericRecord(schema, p1, p2, p3, p4),
        p1, p2, p3, p4);
  }

  public static final AvroType<TupleN> tuples(PType... ptypes) {
    Schema schema = createTupleSchema(ptypes);
    return new AvroType(TupleN.class, schema,
        new GenericRecordToTuple(TupleFactory.TUPLEN, ptypes),
        new TupleToGenericRecord(schema, ptypes), ptypes);
  }
  
  public static <T extends Tuple> AvroType<T> tuples(Class<T> clazz, PType... ptypes) {
    Schema schema = createTupleSchema(ptypes);
    Class[] typeArgs = new Class[ptypes.length];
    for (int i = 0; i < typeArgs.length; i++) {
      typeArgs[i] = ptypes[i].getTypeClass();
    }
    TupleFactory<T> factory = TupleFactory.create(clazz, typeArgs);
    return new AvroType<T>(clazz, schema,
        new GenericRecordToTuple(factory, ptypes), new TupleToGenericRecord(schema, ptypes),
        ptypes);  
  }
  
  private static Schema createTupleSchema(PType... ptypes) {
	// Guarantee each tuple schema has a globally unique name
	String tupleName = "tuple" + UUID.randomUUID().toString().replace('-', 'x');
    Schema schema = Schema.createRecord(tupleName, "", "crunch", false);
    List<Schema.Field> fields = Lists.newArrayList();
    for (int i = 0; i < ptypes.length; i++) {
      AvroType atype = (AvroType) ptypes[i];
      Schema fieldSchema = Schema.createUnion(
          ImmutableList.of(atype.getSchema(), Schema.create(Type.NULL)));
      fields.add(new Schema.Field("v" + i, fieldSchema, "", null));
    }
    schema.setFields(fields);
    return schema;
  }
  
  public static final <S, T> AvroType<T> derived(Class<T> clazz, MapFn<S, T> inputFn,
      MapFn<T, S> outputFn, PType<S> base) {
    AvroType<S> abase = (AvroType<S>) base;
    return new AvroType<T>(clazz, abase.getSchema(),
        new CompositeMapFn(abase.getInputMapFn(), inputFn),
        new CompositeMapFn(outputFn, abase.getOutputMapFn()),
        base.getSubTypes().toArray(new PType[0]));
  }
  
  public static <T> PType<T> jsons(Class<T> clazz) {
    return PTypes.jsonString(clazz, AvroTypeFamily.getInstance());  
  }
  
  public static final <K, V> AvroTableType<K, V> tableOf(PType<K> key, PType<V> value) {
    AvroType<K> avroKey = (AvroType<K>) key;
    AvroType<V> avroValue = (AvroType<V>) value;    
    return new AvroTableType(avroKey, avroValue, Pair.class);
  }

  private Avros() {}
}
