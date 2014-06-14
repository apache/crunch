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
package org.apache.crunch.scrunch;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.hadoop.util.ReflectionUtils;

import scala.collection.JavaConversions;

public class ScalaSafeReflectDatumReader<T> extends ReflectDatumReader<T> {
  
  public ScalaSafeReflectDatumReader(Schema schema) {
    super(schema, schema, ScalaSafeReflectData.getInstance());
  }
  
  @Override
  protected Object readArray(Object old, Schema expected,
      ResolvingDecoder in) throws IOException {
    return scalaIterableCheck(super.readArray(old, expected, in), expected);
  }
  
  @Override
  protected Object readMap(Object old, Schema expected,
      ResolvingDecoder in) throws IOException {
    return scalaMapCheck(super.readMap(old, expected, in), expected);
  }
  
  public static Object scalaMapCheck(Object map, Schema schema) {
    Class mapClass = ScalaSafeReflectData.getClassProp(schema,
        ScalaSafeReflectData.CLASS_PROP);
    if (mapClass != null && mapClass.isAssignableFrom(scala.collection.Map.class)) {
      return JavaConversions.mapAsScalaMap((Map) map);
    }
    return map;
  }
  
  public static Object scalaIterableCheck(Object array, Schema schema) {
    Class collectionClass = ScalaSafeReflectData.getClassProp(schema,
        ScalaSafeReflectData.CLASS_PROP);
    if (collectionClass != null) {
      if (scala.collection.Iterable.class.isAssignableFrom(collectionClass)) {
        scala.collection.Iterable it = toIter(array);
        if (scala.collection.immutable.List.class.isAssignableFrom(collectionClass)) {
          return it.toList();
        }
        if (scala.collection.mutable.Buffer.class.isAssignableFrom(collectionClass)) {
          return it.toBuffer();
        }
        if (scala.collection.immutable.Set.class.isAssignableFrom(collectionClass)) {
          return it.toSet();
        }
        return it;
      }
    }
    return array;
  }
  
  private static scala.collection.Iterable toIter(Object array) {
    return JavaConversions.collectionAsScalaIterable((Collection) array);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Object newArray(Object old, int size, Schema schema) {
    Class collectionClass = ScalaSafeReflectData.getClassProp(schema,
        ScalaSafeReflectData.CLASS_PROP);
    Class elementClass = ScalaSafeReflectData.getClassProp(schema,
        ScalaSafeReflectData.ELEMENT_PROP);
    if (collectionClass == null && elementClass == null)
      return super.newArray(old, size, schema); // use specific/generic
    ScalaSafeReflectData data = ScalaSafeReflectData.getInstance();

    if (collectionClass != null && !collectionClass.isArray()) {
      if (old instanceof Collection) {
        ((Collection)old).clear();
        return old;
      }
      if (scala.collection.Iterable.class.isAssignableFrom(collectionClass) ||
          collectionClass.isAssignableFrom(ArrayList.class)) {
        return Lists.newArrayList();
      }
      return data.newInstance(collectionClass, schema);
    }
    if (elementClass == null) {
      elementClass = data.getClass(schema.getElementType());
    }
    return Array.newInstance(elementClass, size);
  }
}
