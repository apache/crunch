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
package org.apache.crunch.impl.spark.serde;

import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroMode;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.writable.WritableType;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

public class SerDeFactory {
  public static SerDe create(PType<?> ptype, Configuration conf) {
    if (WritableTypeFamily.getInstance().equals(ptype.getFamily())) {
      return new WritableSerDe(((WritableType) ptype).getSerializationClass());
    } else {
      AvroType at = (AvroType) ptype;
      Map<String, String> props = AvroMode.fromType(at).withFactoryFromConfiguration(conf).getModeProperties();
      return new AvroSerDe(at, props);
    }
  }
}
