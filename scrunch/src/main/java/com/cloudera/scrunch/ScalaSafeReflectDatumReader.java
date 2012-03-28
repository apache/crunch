/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
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
package com.cloudera.scrunch;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.reflect.ReflectDatumReader;

import scala.collection.JavaConversions;

/**
 *
 */
public class ScalaSafeReflectDatumReader<T> extends ReflectDatumReader<T> {
  
  public ScalaSafeReflectDatumReader(Schema schema) {
    super(schema, schema, ScalaSafeReflectData.get());
  }
  
  @Override
  protected Object readArray(Object old, Schema expected,
      ResolvingDecoder in) throws IOException {
    Schema expectedType = expected.getElementType();
    long l = in.readArrayStart();
    long base = 0;
    if (l > 0) {
      Object array = newArray(old, (int) l, expected);
      do {
        for (long i = 0; i < l; i++) {
          addToArray(array, base + i, read(peekArray(array), expectedType, in));
        }
        base += l;
      } while ((l = in.arrayNext()) > 0);
      return scalaIterableCheck(array, expected);
    } else {
      return scalaIterableCheck(newArray(old, 0, expected), expected);
    }
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
        } else if (scala.collection.mutable.Buffer.class.isAssignableFrom(collectionClass)) {
          return it.toBuffer();
        } else if (scala.collection.immutable.Set.class.isAssignableFrom(collectionClass)) {
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
  @SuppressWarnings(value="unchecked")
  protected Object newArray(Object old, int size, Schema schema) {
    ScalaSafeReflectData data = ScalaSafeReflectData.get();
    Class collectionClass = ScalaSafeReflectData.getClassProp(schema,
        ScalaSafeReflectData.CLASS_PROP);
    if (collectionClass != null) {
      if (old instanceof Collection) {
        ((Collection)old).clear();
        return old;
      }
      if (scala.collection.Iterable.class.isAssignableFrom(collectionClass) ||
          collectionClass.isAssignableFrom(ArrayList.class)) {
        return new ArrayList();
      }
      return data.newInstance(collectionClass, schema);
    }
    Class elementClass = ScalaSafeReflectData.getClassProp(schema,
        ScalaSafeReflectData.ELEMENT_PROP);
    if (elementClass == null)
      elementClass = data.getClass(schema.getElementType());
    return Array.newInstance(elementClass, size);
  }
}
