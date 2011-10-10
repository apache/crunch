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

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;

import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.SourceTarget;
import com.cloudera.crunch.fn.IdentityFn;
import com.cloudera.crunch.io.avro.AvroFileSourceTarget;
import com.cloudera.crunch.type.Converter;
import com.cloudera.crunch.type.DataBridge;
import com.cloudera.crunch.type.PType;
import com.cloudera.crunch.type.PTypeFamily;
import com.google.common.collect.ImmutableList;

/**
 *
 *
 */
public class AvroType<T> implements PType<T> {

  private static final Converter AVRO_CONVERTER = new AvroKeyConverter();
  
  private final Class<T> typeClass;
  private final Schema schema;
  private final DataBridge handler;
  private final MapFn baseInputMapFn;
  private final MapFn baseOutputMapFn;
  private final List<PType> subTypes;
  
  public AvroType(Class<T> typeClass, Schema schema, PType... ptypes) {
    this(typeClass, schema, IdentityFn.getInstance(),
        IdentityFn.getInstance(), ptypes);
  }
  
  public AvroType(Class<T> typeClass, Schema schema,
      MapFn inputMapFn, MapFn outputMapFn, PType...ptypes) {
    this.typeClass = typeClass;
    this.schema = schema;
    this.baseInputMapFn = inputMapFn;
    this.baseOutputMapFn = outputMapFn;
    this.handler = new DataBridge(AvroWrapper.class, NullWritable.class, AVRO_CONVERTER,
        new InputWrapperMapFn(inputMapFn), new OutputWrapperMapFn(outputMapFn));
    this.subTypes = ImmutableList.<PType>builder().add(ptypes).build();
  }
  
  @Override
  public Class<T> getTypeClass() {
    return typeClass;
  }
  
  @Override
  public PTypeFamily getFamily() {
    return AvroTypeFamily.getInstance();
  }

  @Override
  public List<PType> getSubTypes() {
    return subTypes;
  }
  
  public Schema getSchema() {
    return schema;
  }

  public MapFn getBaseInputMapFn() {
    return baseInputMapFn;
  }
  
  public MapFn getBaseOutputMapFn() {
    return baseOutputMapFn;
  }
  
  private static class InputWrapperMapFn<V, S> extends MapFn<AvroWrapper<V>, S> {
    private final MapFn<V, S> map;
    
    public InputWrapperMapFn(MapFn<V, S> map) {
      this.map = map;
    }
    
    @Override
    public void initialize() {
      map.initialize();
    }
    
    @Override
    public S map(AvroWrapper<V> input) {
      return map.map(input.datum());
    }
  }
  
  private static class OutputWrapperMapFn<S, V> extends MapFn<S, AvroWrapper<V>> {
    private final MapFn<S, V> map;
    private transient AvroWrapper<V> wrapper;
    
    public OutputWrapperMapFn(MapFn<S, V> map) {
      this.map = map;
    }
    
    @Override
    public void initialize() {
      this.wrapper = new AvroWrapper<V>();
      this.map.initialize();
    }
    
    @Override
    public AvroWrapper<V> map(S input) {
      wrapper.datum(map.map(input));
      return wrapper;
    } 
  }

  @Override
  public DataBridge getDataBridge() {
    return handler;
  }

  @Override
  public SourceTarget<T> getDefaultFileSource(Path path) {
    return new AvroFileSourceTarget<T>(path, this);
  }
  
  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof AvroType)) {
      return false;
    }
    AvroType at = (AvroType) other;
    if (subTypes.size() == at.subTypes.size()) {
      for (int i = 0; i < subTypes.size(); i++) {
        if (!subTypes.get(i).equals(at.subTypes.get(i))) {
          return false;
        }
      }
    }
    return this == other;
  }
  
  @Override
  public int hashCode() {
    return 17 + 37*subTypes.hashCode();
  }
}
