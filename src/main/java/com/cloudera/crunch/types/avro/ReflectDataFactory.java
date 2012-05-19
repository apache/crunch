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
package com.cloudera.crunch.types.avro;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

/**
 * A Factory class for constructing Avro reflection-related objects.
 */
public class ReflectDataFactory {

  public ReflectData getReflectData() { return ReflectData.AllowNull.get(); }
  
  public <T> ReflectDatumReader<T> getReader(Schema schema) {
    return new ReflectDatumReader<T>(schema);
  }
  
  public <T> ReflectDatumWriter<T> getWriter() {
    return new ReflectDatumWriter<T>();
  }
}
