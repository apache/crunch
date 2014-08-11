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
package org.apache.crunch.types.orc;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.apache.crunch.MapFn;
import org.apache.crunch.Tuple;
import org.apache.crunch.TupleN;
import org.apache.crunch.fn.CompositeMapFn;
import org.apache.crunch.io.orc.OrcWritable;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.TupleFactory;
import org.apache.crunch.types.writable.WritableType;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * Utilities to create PTypes for ORC serialization / deserialization
 * 
 */
public class Orcs {
  
  /**
   * Create a PType to directly use OrcStruct as the deserialized format. This
   * is the fastest way for serialization/deserializations. However, users
   * need to use ObjectInspectors to handle the OrcStruct. Currently, void and
   * union types are not supported.
   * 
   * @param typeInfo
   * @return
   */
  public static final PType<OrcStruct> orcs(TypeInfo typeInfo) {
    return Writables.derived(OrcStruct.class, new OrcInFn(typeInfo), new OrcOutFn(typeInfo),
        Writables.writables(OrcWritable.class));
  }
  
  /**
   * Create a PType which uses reflection to serialize/deserialize java POJOs
   * to/from ORC. There are some restrictions of the POJO: 1) it must have a
   * default, no-arg constructor; 2) All of its fields must be Hive primitive
   * types or collection types that have Hive equivalents; 3) Void and Union
   * are not supported yet.
   * 
   * @param clazz
   * @return
   */
  public static final <T> PType<T> reflects(Class<T> clazz) {
    TypeInfo reflectInfo = createReflectTypeInfo(clazz);
    return Writables.derived(clazz, new ReflectInFn<T>(clazz),
        new ReflectOutFn<T>(clazz), orcs(reflectInfo));
  }
  
  private static TypeInfo createReflectTypeInfo(Class<?> clazz) {
    ObjectInspector reflectOi = ObjectInspectorFactory
        .getReflectionObjectInspector(clazz, ObjectInspectorOptions.JAVA);
    return TypeInfoUtils.getTypeInfoFromObjectInspector(reflectOi);
  }
  
  /**
   * Create a tuple-based PType. Users can use other Crunch PTypes (such as
   * Writables.ints(), Orcs.reflects(), Writables.pairs(), ...) to construct
   * the PType. Currently, nulls and unions are not supported.
   * 
   * @param ptypes
   * @return
   */
  public static final PType<TupleN> tuples(PType... ptypes) {
    TypeInfo tupleInfo = createTupleTypeInfo(ptypes);
    return derived(TupleN.class, new TupleInFn<TupleN>(TupleFactory.TUPLEN, ptypes),
        new TupleOutFn<TupleN>(ptypes), orcs(tupleInfo), ptypes);
  }
  
  // derived, but override subtypes
  static <S, T> PType<T> derived(Class<T> clazz, MapFn<S, T> inputFn, MapFn<T, S> outputFn,
      PType<S> base, PType[] subTypes) {
    WritableType<S, ?> wt = (WritableType<S, ?>) base;
    MapFn input = new CompositeMapFn(wt.getInputMapFn(), inputFn);
    MapFn output = new CompositeMapFn(outputFn, wt.getOutputMapFn());
    return new WritableType(clazz, wt.getSerializationClass(), input, output, subTypes);
  }
  
  private static TypeInfo createTupleTypeInfo(PType... ptypes) {
    ObjectInspector tupleOi = new TupleObjectInspector(null, ptypes);
    return TypeInfoUtils.getTypeInfoFromObjectInspector(tupleOi);
  }
  
  private static class OrcInFn extends MapFn<OrcWritable, OrcStruct> {
    
    private TypeInfo typeInfo;
    
    private transient ObjectInspector oi;
    private transient BinarySortableSerDe serde;

    public OrcInFn(TypeInfo typeInfo) {
      this.typeInfo = typeInfo;
    }
    
    @Override
    public void initialize() {
      oi = OrcStruct.createObjectInspector(typeInfo);
      serde = OrcUtils.createBinarySerde(typeInfo);
    }
    
    @Override
    public OrcStruct map(OrcWritable input) {
      input.setObjectInspector(oi);
      input.setSerde(serde);
      return input.get();
    }
    
  }
  
  private static class OrcOutFn extends MapFn<OrcStruct, OrcWritable> {
    
    private TypeInfo typeInfo;
    
    private transient ObjectInspector oi;
    private transient BinarySortableSerDe serde;
    
    public OrcOutFn(TypeInfo typeInfo) {
      this.typeInfo = typeInfo;
    }
    
    @Override
    public void initialize() {
      oi = OrcStruct.createObjectInspector(typeInfo);
      serde = OrcUtils.createBinarySerde(typeInfo);
    }

    @Override
    public OrcWritable map(OrcStruct input) {
      OrcWritable output = new OrcWritable();
      output.setObjectInspector(oi);
      output.setSerde(serde);
      output.set(input);
      return output;
    }
    
  }
  
  private static Map<Class<?>, Field[]> fieldsCache = new HashMap<Class<?>, Field[]>();
  
  private static class ReflectInFn<T> extends MapFn<OrcStruct, T> {
    
    private Class<T> typeClass;
    private TypeInfo typeInfo;
    
    private transient ObjectInspector reflectOi;
    private transient ObjectInspector orcOi;
    
    @Override
    public void initialize() {
      reflectOi = ObjectInspectorFactory
          .getReflectionObjectInspector(typeClass, ObjectInspectorOptions.JAVA);
      orcOi = OrcStruct.createObjectInspector(typeInfo);
    }
    
    public ReflectInFn(Class<T> typeClass) {
      this.typeClass = typeClass;
      typeInfo = createReflectTypeInfo(typeClass);
    }

    @Override
    public T map(OrcStruct input) {
      return (T) OrcUtils.convert(input, orcOi, reflectOi);
    }
    
  }
  
  private static class ReflectOutFn<T> extends MapFn<T, OrcStruct> {
    
    private Class<T> typeClass;
    private TypeInfo typeInfo;
    
    private transient ObjectInspector reflectOi;
    private transient SettableStructObjectInspector orcOi;
    
    @Override
    public void initialize() {
      reflectOi = ObjectInspectorFactory.getReflectionObjectInspector(typeClass,
          ObjectInspectorOptions.JAVA);
      orcOi = (SettableStructObjectInspector) OrcStruct.createObjectInspector(typeInfo);
    }
    
    public ReflectOutFn(Class<T> typeClass) {
      this.typeClass = typeClass;
      typeInfo = createReflectTypeInfo(typeClass);
    }

    @Override
    public OrcStruct map(T input) {
      return (OrcStruct) OrcUtils.convert(input, reflectOi, orcOi);
    }
    
  }
  
  private static class TupleInFn<T extends Tuple> extends MapFn<OrcStruct, T> {
    
    private PType[] ptypes;
    private TupleFactory<T> tupleFactory;
    
    private transient ObjectInspector tupleOi;
    private transient ObjectInspector orcOi;
    
    public TupleInFn(TupleFactory<T> tupleFactory, PType... ptypes) {
      this.tupleFactory = tupleFactory;
      this.ptypes = ptypes;
    }
    
    @Override
    public void initialize() {
      tupleOi = new TupleObjectInspector<T>(tupleFactory, ptypes);
      TypeInfo info = TypeInfoUtils.getTypeInfoFromObjectInspector(tupleOi);
      orcOi = OrcStruct.createObjectInspector(info);
    }

    @Override
    public T map(OrcStruct input) {
      return (T) OrcUtils.convert(input, orcOi, tupleOi);
    }
    
  }
  
  private static class TupleOutFn<T extends Tuple> extends MapFn<T, OrcStruct> {
    
    private PType[] ptypes;
    
    private transient ObjectInspector tupleOi;
    private transient ObjectInspector orcOi;
    
    public TupleOutFn(PType... ptypes) {
      this.ptypes = ptypes;
    }
    
    @Override
    public void initialize() {
      tupleOi = new TupleObjectInspector<T>(null, ptypes);
      TypeInfo info = TypeInfoUtils.getTypeInfoFromObjectInspector(tupleOi);
      orcOi = OrcStruct.createObjectInspector(info);
    }

    @Override
    public OrcStruct map(T input) {
      return (OrcStruct) OrcUtils.convert(input, tupleOi, orcOi);
    }
    
  }

}
