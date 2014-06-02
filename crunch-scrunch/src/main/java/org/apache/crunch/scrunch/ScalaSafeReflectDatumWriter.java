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

import java.util.Iterator;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectDatumWriter;

import scala.collection.JavaConversions;

/**
 *
 */
public class ScalaSafeReflectDatumWriter<T> extends ReflectDatumWriter<T> {
  public ScalaSafeReflectDatumWriter(Schema schema) {
    super(schema, ScalaSafeReflectData.getInstance());
  }

  @Override
  protected long getArraySize(Object array) {
    if (array instanceof scala.collection.Iterable) {
      return ((scala.collection.Iterable) array).size();
    }
    return super.getArraySize(array);
  }

  @Override
  protected Iterator<Object> getArrayElements(Object array) {
    if (array instanceof scala.collection.Iterable) {
      return JavaConversions.asJavaIterable((scala.collection.Iterable) array).iterator(); 
    }
    return (Iterator<Object>) super.getArrayElements(array);
  }

  @Override
  protected int getMapSize(Object map) {
    if (map instanceof scala.collection.Map) {
      return ((scala.collection.Map) map).size();
    }
    return super.getMapSize(map);
  }

  /** Called by the default implementation of {@link #writeMap} to enumerate
   * map elements.  The default implementation is for {@link Map}.*/
  @SuppressWarnings("unchecked")
  protected Iterable<Map.Entry<Object,Object>> getMapEntries(Object map) {
    if (map instanceof scala.collection.Map) {
      return JavaConversions.mapAsJavaMap((scala.collection.Map) map).entrySet();
    }
    return super.getMapEntries(map);
  }
}
