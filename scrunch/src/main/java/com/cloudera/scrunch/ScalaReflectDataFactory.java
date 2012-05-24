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

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import com.cloudera.crunch.types.avro.ReflectDataFactory;

/**
 * An implementation of the {@code ReflectDataFactory} class to work with Scala classes.
 */
public class ScalaReflectDataFactory extends ReflectDataFactory {

  public ReflectData getReflectData() { return ScalaSafeReflectData.get(); }
  
  public <T> ReflectDatumReader<T> getReader(Schema schema) {
    return new ScalaSafeReflectDatumReader<T>(schema);
  }
  
  public <T> ReflectDatumWriter<T> getWriter() {
    return new ScalaSafeReflectDatumWriter<T>();
  }
}
