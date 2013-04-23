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
package org.apache.crunch.types.avro;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;
import org.apache.crunch.Tuple;
import org.apache.crunch.Tuple3;
import org.apache.crunch.Tuple4;
import org.apache.crunch.TupleN;
import org.apache.crunch.fn.CompositeMapFn;
import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.types.CollectionDeepCopier;
import org.apache.crunch.types.DeepCopier;
import org.apache.crunch.types.MapDeepCopier;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypes;
import org.apache.crunch.types.TupleDeepCopier;
import org.apache.crunch.types.TupleFactory;
import org.apache.crunch.types.writable.WritableDeepCopier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.ReflectionUtils;

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

  /**
   * Older versions of Avro (i.e., before 1.7.0) do not support schemas that are
   * composed of a mix of specific and reflection-based schemas. This bit
   * controls whether or not we allow Crunch jobs to be created that involve
   * mixing specific and reflection-based schemas and can be overridden by the
   * client developer.
   */
  public static final boolean CAN_COMBINE_SPECIFIC_AND_REFLECT_SCHEMAS;

  static {
    CAN_COMBINE_SPECIFIC_AND_REFLECT_SCHEMAS = AvroCapabilities.canDecodeSpecificSchemaWithReflectDatumReader();
  }

  /**
   * The instance we use for generating reflected schemas. May be modified by
   * clients (e.g., Scrunch.)
   */
  public static ReflectDataFactory REFLECT_DATA_FACTORY = new ReflectDataFactory();

  /**
   * The name of the configuration parameter that tracks which reflection
   * factory to use.
   */
  public static final String REFLECT_DATA_FACTORY_CLASS = "crunch.reflectdatafactory";

  public static void configureReflectDataFactory(Configuration conf) {
    conf.setClass(REFLECT_DATA_FACTORY_CLASS, REFLECT_DATA_FACTORY.getClass(), ReflectDataFactory.class);
  }

  public static ReflectDataFactory getReflectDataFactory(Configuration conf) {
    return (ReflectDataFactory) ReflectionUtils.newInstance(
        conf.getClass(REFLECT_DATA_FACTORY_CLASS, ReflectDataFactory.class), conf);
  }

  public static void checkCombiningSpecificAndReflectionSchemas() {
    if (!CAN_COMBINE_SPECIFIC_AND_REFLECT_SCHEMAS) {
      throw new IllegalStateException("Crunch does not support running jobs that"
          + " contain a mixture of reflection-based and avro-generated data types."
          + " Please consider turning your reflection-based type into an avro-generated"
          + " type and using that generated type instead."
          + " If the version of Avro you are using is 1.7.0 or greater, you can enable"
          + " combined schemas by setting the Avros.CAN_COMBINE_SPECIFIC_AND_REFLECT_SCHEMAS" + " field to 'true'.");
    }
  }

  public static MapFn<CharSequence, String> UTF8_TO_STRING = new MapFn<CharSequence, String>() {
    @Override
    public String map(CharSequence input) {
      return input.toString();
    }
  };

  public static MapFn<String, Utf8> STRING_TO_UTF8 = new MapFn<String, Utf8>() {
    @Override
    public Utf8 map(String input) {
      return new Utf8(input);
    }
  };

  public static MapFn<Object, ByteBuffer> BYTES_IN = new MapFn<Object, ByteBuffer>() {
    @Override
    public ByteBuffer map(Object input) {
      if (input instanceof ByteBuffer) {
        return (ByteBuffer) input;
      }
      return ByteBuffer.wrap((byte[]) input);
    }
  };

  private static final AvroType<String> strings = new AvroType<String>(String.class, Schema.create(Schema.Type.STRING),
      UTF8_TO_STRING, STRING_TO_UTF8, new DeepCopier.NoOpDeepCopier<String>());
  private static final AvroType<Void> nulls = create(Void.class, Schema.Type.NULL);
  private static final AvroType<Long> longs = create(Long.class, Schema.Type.LONG);
  private static final AvroType<Integer> ints = create(Integer.class, Schema.Type.INT);
  private static final AvroType<Float> floats = create(Float.class, Schema.Type.FLOAT);
  private static final AvroType<Double> doubles = create(Double.class, Schema.Type.DOUBLE);
  private static final AvroType<Boolean> booleans = create(Boolean.class, Schema.Type.BOOLEAN);
  private static final AvroType<ByteBuffer> bytes = new AvroType<ByteBuffer>(ByteBuffer.class,
      Schema.create(Schema.Type.BYTES), BYTES_IN, IdentityFn.getInstance(), new DeepCopier.NoOpDeepCopier<ByteBuffer>());

  private static final Map<Class<?>, PType<?>> PRIMITIVES = ImmutableMap.<Class<?>, PType<?>> builder()
      .put(String.class, strings).put(Long.class, longs).put(Integer.class, ints).put(Float.class, floats)
      .put(Double.class, doubles).put(Boolean.class, booleans).put(ByteBuffer.class, bytes).build();

  private static final Map<Class<?>, AvroType<?>> EXTENSIONS = Maps.newHashMap();

  public static <T> void register(Class<T> clazz, AvroType<T> ptype) {
    EXTENSIONS.put(clazz, ptype);
  }

  public static <T> PType<T> getPrimitiveType(Class<T> clazz) {
    return (PType<T>) PRIMITIVES.get(clazz);
  }

  static <T> boolean isPrimitive(AvroType<T> avroType) {
    return avroType.getTypeClass().isPrimitive() || PRIMITIVES.containsKey(avroType.getTypeClass());
  }

  private static <T> AvroType<T> create(Class<T> clazz, Schema.Type schemaType) {
    return new AvroType<T>(clazz, Schema.create(schemaType), new DeepCopier.NoOpDeepCopier<T>());
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
    return new AvroType<GenericData.Record>(GenericData.Record.class, schema, new AvroDeepCopier.AvroGenericDeepCopier(
        schema));
  }

  public static final <T> AvroType<T> containers(Class<T> clazz) {
    if (SpecificRecord.class.isAssignableFrom(clazz)) {
      return (AvroType<T>) specifics((Class<SpecificRecord>) clazz);
    }
    return reflects(clazz);
  }

  public static final <T extends SpecificRecord> AvroType<T> specifics(Class<T> clazz) {
    T t = ReflectionUtils.newInstance(clazz, null);
    Schema schema = t.getSchema();
    return new AvroType<T>(clazz, schema, new AvroDeepCopier.AvroSpecificDeepCopier<T>(clazz, schema));
  }

  public static final <T> AvroType<T> reflects(Class<T> clazz) {
    Schema schema = REFLECT_DATA_FACTORY.getReflectData().getSchema(clazz);
    return new AvroType<T>(clazz, schema, new AvroDeepCopier.AvroReflectDeepCopier<T>(clazz, schema));
  }

  private static class BytesToWritableMapFn<T extends Writable> extends MapFn<Object, T> {
    private static final Log LOG = LogFactory.getLog(BytesToWritableMapFn.class);

    private final Class<T> writableClazz;

    public BytesToWritableMapFn(Class<T> writableClazz) {
      this.writableClazz = writableClazz;
    }

    @Override
    public T map(Object input) {
      ByteBuffer byteBuffer = BYTES_IN.map(input);
      T instance = ReflectionUtils.newInstance(writableClazz, null);
      try {
        instance.readFields(new DataInputStream(new ByteArrayInputStream(byteBuffer.array(),
            byteBuffer.arrayOffset(), byteBuffer.limit())));
      } catch (IOException e) {
        LOG.error("Exception thrown reading instance of: " + writableClazz, e);
      }
      return instance;
    }
  }

  private static class WritableToBytesMapFn<T extends Writable> extends MapFn<T, ByteBuffer> {
    private static final Log LOG = LogFactory.getLog(WritableToBytesMapFn.class);

    @Override
    public ByteBuffer map(T input) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream das = new DataOutputStream(baos);
      try {
        input.write(das);
      } catch (IOException e) {
        LOG.error("Exception thrown converting Writable to bytes", e);
      }
      return ByteBuffer.wrap(baos.toByteArray());
    }
  }

  public static final <T extends Writable> AvroType<T> writables(Class<T> clazz) {
    return new AvroType<T>(clazz, Schema.create(Schema.Type.BYTES), new BytesToWritableMapFn<T>(clazz),
        new WritableToBytesMapFn<T>(), new WritableDeepCopier<T>(clazz));
  }

  private static class GenericDataArrayToCollection<T> extends MapFn<Object, Collection<T>> {

    private final MapFn<Object, T> mapFn;

    public GenericDataArrayToCollection(MapFn<Object, T> mapFn) {
      this.mapFn = mapFn;
    }

    @Override
    public void configure(Configuration conf) {
      mapFn.configure(conf);
    }

    @Override
    public void setContext(TaskInputOutputContext<?, ?, ?, ?> context) {
      mapFn.setContext(context);
    }
    
    @Override
    public void initialize() {
      mapFn.initialize();
    }

    @Override
    public Collection<T> map(Object input) {
      Collection<T> ret = Lists.newArrayList();
      if (input instanceof Collection) {
        for (Object in : (Collection<Object>) input) {
          ret.add(mapFn.map(in));
        }
      } else {
        // Assume it is an array
        Object[] arr = (Object[]) input;
        for (Object in : arr) {
          ret.add(mapFn.map(in));
        }
      }
      return ret;
    }
  }

  private static class CollectionToGenericDataArray extends MapFn<Collection<?>, GenericData.Array<?>> {

    private final MapFn mapFn;
    private final String jsonSchema;
    private transient Schema schema;

    public CollectionToGenericDataArray(Schema schema, MapFn mapFn) {
      this.mapFn = mapFn;
      this.jsonSchema = schema.toString();
    }

    @Override
    public void configure(Configuration conf) {
      mapFn.configure(conf);
    }

    @Override
    public void setContext(TaskInputOutputContext<?, ?, ?, ?> context) {
      mapFn.setContext(context);
    }
    
    @Override
    public void initialize() {
      mapFn.initialize();
    }

    @Override
    public GenericData.Array<?> map(Collection<?> input) {
      if (schema == null) {
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
    Schema collectionSchema = Schema.createArray(allowNulls(avroType.getSchema()));
    GenericDataArrayToCollection<T> input = new GenericDataArrayToCollection<T>(avroType.getInputMapFn());
    CollectionToGenericDataArray output = new CollectionToGenericDataArray(collectionSchema, avroType.getOutputMapFn());
    return new AvroType(Collection.class, collectionSchema, input, output, new CollectionDeepCopier<T>(ptype), ptype);
  }

  private static class AvroMapToMap<T> extends MapFn<Map<CharSequence, Object>, Map<String, T>> {
    private final MapFn<Object, T> mapFn;

    public AvroMapToMap(MapFn<Object, T> mapFn) {
      this.mapFn = mapFn;
    }

    @Override
    public void configure(Configuration conf) {
      mapFn.configure(conf);
    }

    @Override
    public void setContext(TaskInputOutputContext<?, ?, ?, ?> context) {
      mapFn.setContext(context);
    }
    
    @Override
    public void initialize() {
      mapFn.initialize();
    }

    @Override
    public Map<String, T> map(Map<CharSequence, Object> input) {
      Map<String, T> out = Maps.newHashMap();
      for (Map.Entry<CharSequence, Object> e : input.entrySet()) {
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
    public void configure(Configuration conf) {
      mapFn.configure(conf);
    }

    @Override
    public void setContext(TaskInputOutputContext<?, ?, ?, ?> context) {
      mapFn.setContext(context);
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
    Schema mapSchema = Schema.createMap(allowNulls(avroType.getSchema()));
    AvroMapToMap<T> inputFn = new AvroMapToMap<T>(avroType.getInputMapFn());
    MapToAvroMap<T> outputFn = new MapToAvroMap<T>(avroType.getOutputMapFn());
    return new AvroType(Map.class, mapSchema, inputFn, outputFn, new MapDeepCopier<T>(ptype), ptype);
  }

  private static class GenericRecordToTuple extends MapFn<GenericRecord, Tuple> {
    private final TupleFactory<?> tupleFactory;
    private final List<MapFn> fns;

    private transient Object[] values;

    public GenericRecordToTuple(TupleFactory<?> tupleFactory, PType<?>... ptypes) {
      this.tupleFactory = tupleFactory;
      this.fns = Lists.newArrayList();
      for (PType<?> ptype : ptypes) {
        AvroType atype = (AvroType) ptype;
        fns.add(atype.getInputMapFn());
      }
    }

    @Override
    public void configure(Configuration conf) {
      for (MapFn fn : fns) {
        fn.configure(conf);
      }
    }

    @Override
    public void setContext(TaskInputOutputContext<?, ?, ?, ?> context) {
      for (MapFn fn : fns) {
        fn.setContext(context);
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
    private final List<AvroType> avroTypes;
    private final String jsonSchema;
    private final boolean isReflect;
    private transient Schema schema;

    public TupleToGenericRecord(Schema schema, PType<?>... ptypes) {
      this.fns = Lists.newArrayList();
      this.avroTypes = Lists.newArrayList();
      this.jsonSchema = schema.toString();
      boolean reflectFound = false;
      boolean specificFound = false;
      for (PType ptype : ptypes) {
        AvroType atype = (AvroType) ptype;
        fns.add(atype.getOutputMapFn());
        avroTypes.add(atype);
        if (atype.hasReflect()) {
          reflectFound = true;
        }
        if (atype.hasSpecific()) {
          specificFound = true;
        }
      }
      if (specificFound && reflectFound) {
        checkCombiningSpecificAndReflectionSchemas();
      }
      this.isReflect = reflectFound;
    }

    @Override
    public void configure(Configuration conf) {
      for (MapFn fn : fns) {
        fn.configure(conf);
      }
    }
 
    @Override
    public void setContext(TaskInputOutputContext<?, ?, ?, ?> context) {
      for (MapFn fn : fns) {
        fn.setContext(getContext());
      }
    }
    
    @Override
    public void initialize() {
      this.schema = new Schema.Parser().parse(jsonSchema);
      for (MapFn fn : fns) {
        fn.initialize();
      }
    }

    private GenericRecord createRecord() {
      if (isReflect) {
        return new ReflectGenericRecord(schema);
      } else {
        return new GenericData.Record(schema);
      }
    }

    @Override
    public GenericRecord map(Tuple input) {
      GenericRecord record = createRecord();
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
    TupleToGenericRecord output = new TupleToGenericRecord(schema, p1, p2);
    return new AvroType(Pair.class, schema, input, output, new TupleDeepCopier(Pair.class, p1, p2), p1, p2);
  }

  public static final <V1, V2, V3> AvroType<Tuple3<V1, V2, V3>> triples(PType<V1> p1, PType<V2> p2, PType<V3> p3) {
    Schema schema = createTupleSchema(p1, p2, p3);
    return new AvroType(Tuple3.class, schema, new GenericRecordToTuple(TupleFactory.TUPLE3, p1, p2, p3),
        new TupleToGenericRecord(schema, p1, p2, p3), new TupleDeepCopier(Tuple3.class, p1, p2, p3), p1, p2, p3);
  }

  public static final <V1, V2, V3, V4> AvroType<Tuple4<V1, V2, V3, V4>> quads(PType<V1> p1, PType<V2> p2, PType<V3> p3,
      PType<V4> p4) {
    Schema schema = createTupleSchema(p1, p2, p3, p4);
    return new AvroType(Tuple4.class, schema, new GenericRecordToTuple(TupleFactory.TUPLE4, p1, p2, p3, p4),
        new TupleToGenericRecord(schema, p1, p2, p3, p4), new TupleDeepCopier(Tuple4.class, p1, p2, p3, p4), p1, p2,
        p3, p4);
  }

  public static final AvroType<TupleN> tuples(PType... ptypes) {
    Schema schema = createTupleSchema(ptypes);
    return new AvroType(TupleN.class, schema, new GenericRecordToTuple(TupleFactory.TUPLEN, ptypes),
        new TupleToGenericRecord(schema, ptypes), new TupleDeepCopier(TupleN.class, ptypes), ptypes);
  }

  public static <T extends Tuple> AvroType<T> tuples(Class<T> clazz, PType... ptypes) {
    Schema schema = createTupleSchema(ptypes);
    Class[] typeArgs = new Class[ptypes.length];
    for (int i = 0; i < typeArgs.length; i++) {
      typeArgs[i] = ptypes[i].getTypeClass();
    }
    TupleFactory<T> factory = TupleFactory.create(clazz, typeArgs);
    return new AvroType<T>(clazz, schema, new GenericRecordToTuple(factory, ptypes), new TupleToGenericRecord(schema,
        ptypes), new TupleDeepCopier(clazz, ptypes), ptypes);
  }

  private static Schema createTupleSchema(PType<?>... ptypes) {
    // Guarantee each tuple schema has a globally unique name
    String tupleName = "tuple" + UUID.randomUUID().toString().replace('-', 'x');
    Schema schema = Schema.createRecord(tupleName, "", "crunch", false);
    List<Schema.Field> fields = Lists.newArrayList();
    for (int i = 0; i < ptypes.length; i++) {
      AvroType atype = (AvroType) ptypes[i];
      Schema fieldSchema = allowNulls(atype.getSchema());
      fields.add(new Schema.Field("v" + i, fieldSchema, "", null));
    }
    schema.setFields(fields);
    return schema;
  }

  public static final <S, T> AvroType<T> derived(Class<T> clazz, MapFn<S, T> inputFn, MapFn<T, S> outputFn,
      PType<S> base) {
    AvroType<S> abase = (AvroType<S>) base;
    return new AvroType<T>(clazz, abase.getSchema(), new CompositeMapFn(abase.getInputMapFn(), inputFn),
        new CompositeMapFn(outputFn, abase.getOutputMapFn()), new DeepCopier.NoOpDeepCopier<T>(), base.getSubTypes()
            .toArray(new PType[0]));
  }

  public static <T> PType<T> jsons(Class<T> clazz) {
    return PTypes.jsonString(clazz, AvroTypeFamily.getInstance());
  }

  public static final <K, V> AvroTableType<K, V> tableOf(PType<K> key, PType<V> value) {
    if (key instanceof PTableType) {
      PTableType ptt = (PTableType) key;
      key = Avros.pairs(ptt.getKeyType(), ptt.getValueType());
    }
    if (value instanceof PTableType) {
      PTableType ptt = (PTableType) value;
      value = Avros.pairs(ptt.getKeyType(), ptt.getValueType());
    }
    AvroType<K> avroKey = (AvroType<K>) key;
    AvroType<V> avroValue = (AvroType<V>) value;
    return new AvroTableType(avroKey, avroValue, Pair.class);
  }

  private static final Schema NULL_SCHEMA = Schema.create(Type.NULL);

  private static Schema allowNulls(Schema base) {
    if (NULL_SCHEMA.equals(base)) {
      return base;
    }
    return Schema.createUnion(ImmutableList.of(base, NULL_SCHEMA));
  }

  private static class ReflectGenericRecord extends GenericData.Record {

    public ReflectGenericRecord(Schema schema) {
      super(schema);
    }

    @Override
    public int hashCode() {
      return reflectAwareHashCode(this, getSchema());
    }
  }

  /*
   * TODO: Remove this once we no longer have to support 1.5.4.
   */
  private static int reflectAwareHashCode(Object o, Schema s) {
    if (o == null)
      return 0; // incomplete datum
    int hashCode = 1;
    switch (s.getType()) {
    case RECORD:
      for (Schema.Field f : s.getFields()) {
        if (f.order() == Schema.Field.Order.IGNORE)
          continue;
        hashCode = hashCodeAdd(hashCode, ReflectData.get().getField(o, f.name(), f.pos()), f.schema());
      }
      return hashCode;
    case ARRAY:
      Collection<?> a = (Collection<?>) o;
      Schema elementType = s.getElementType();
      for (Object e : a)
        hashCode = hashCodeAdd(hashCode, e, elementType);
      return hashCode;
    case UNION:
      return reflectAwareHashCode(o, s.getTypes().get(ReflectData.get().resolveUnion(s, o)));
    case ENUM:
      return s.getEnumOrdinal(o.toString());
    case NULL:
      return 0;
    case STRING:
      return (o instanceof Utf8 ? o : new Utf8(o.toString())).hashCode();
    default:
      return o.hashCode();
    }
  }

  /** Add the hash code for an object into an accumulated hash code. */
  private static int hashCodeAdd(int hashCode, Object o, Schema s) {
    return 31 * hashCode + reflectAwareHashCode(o, s);
  }

  private Avros() {
  }
}
