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

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryData;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Partitioner;

import com.cloudera.crunch.GroupingOptions.Builder;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.Tuple3;
import com.cloudera.crunch.Tuple4;
import com.cloudera.crunch.TupleN;
import com.cloudera.crunch.type.PGroupedTableType;
import com.cloudera.crunch.type.PTableType;
import com.cloudera.crunch.type.PType;
import com.cloudera.crunch.type.PTypeFamily;
import com.cloudera.crunch.type.PTypeUtils;

public class AvroTypeFamily implements PTypeFamily {
  
  private static final AvroTypeFamily INSTANCE = new AvroTypeFamily();
  
  public static AvroTypeFamily getInstance() {
    return INSTANCE;
  }
  
  // There can only be one instance.
  private AvroTypeFamily() {
  }
  
  @Override
  public PType<String> strings() {
    return Avros.strings();
  }

  @Override
  public PType<Long> longs() {
    return Avros.longs();
  }

  @Override
  public PType<Integer> ints() {
    return Avros.ints();
  }

  @Override
  public PType<Float> floats() {
    return Avros.floats();
  }

  @Override
  public PType<Double> doubles() {
    return Avros.doubles();
  }

  @Override
  public PType<Boolean> booleans() {
    return Avros.booleans();
  }

  @Override
  public PType<ByteBuffer> bytes() {
    return Avros.bytes();
  }

  @Override
  public <T> PType<T> records(Class<T> clazz) {
    return Avros.records(clazz);
  }

  public <T> PType<T> containers(Class<T> clazz, Schema schema) {
    return Avros.containers(clazz, schema);
  }
  
  @Override
  public <T> PType<Collection<T>> collections(PType<T> ptype) {
    return Avros.collections(ptype);
  }

  @Override
  public <V1, V2> PType<Pair<V1, V2>> pairs(PType<V1> p1, PType<V2> p2) {
    return Avros.pairs(p1, p2);
  }

  @Override
  public <V1, V2, V3> PType<Tuple3<V1, V2, V3>> triples(PType<V1> p1,
      PType<V2> p2, PType<V3> p3) {
    return Avros.triples(p1, p2, p3);
  }

  @Override
  public <V1, V2, V3, V4> PType<Tuple4<V1, V2, V3, V4>> quads(PType<V1> p1,
      PType<V2> p2, PType<V3> p3, PType<V4> p4) {
    return Avros.quads(p1, p2, p3, p4);
  }

  @Override
  public PType<TupleN> tuples(PType... ptypes) {
    return Avros.tuples(ptypes);
  }

  @Override
  public <K, V> PTableType<K, V> tableOf(PType<K> key, PType<V> value) {
    return Avros.tableOf(key, value);
  }

  @Override
  public <T> PType<T> as(PType<T> ptype) {
    if (ptype instanceof AvroType || ptype instanceof AvroGroupedTableType) {
      return ptype;
    }
    if (ptype instanceof PGroupedTableType) {
      PTableType ptt = ((PGroupedTableType) ptype).getTableType();
      return new AvroGroupedTableType((AvroTableType) as(ptt));
    }
    Class<T> typeClass = ptype.getTypeClass();
    PType<T> prim = Avros.getPrimitiveType(typeClass);
    if (prim != null) {
      return prim;
    }
    return PTypeUtils.convert(ptype, this);
  }


}
