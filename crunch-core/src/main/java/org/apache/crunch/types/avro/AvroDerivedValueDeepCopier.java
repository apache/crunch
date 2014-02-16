/*
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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.crunch.types.avro;

import org.apache.crunch.MapFn;
import org.apache.crunch.types.DeepCopier;
import org.apache.hadoop.conf.Configuration;

/**
 * A DeepCopier specific to Avro derived types.
 */
public class AvroDerivedValueDeepCopier<T, S> implements DeepCopier {

  private final MapFn<T,S> derivedToAvroFn;
  private final MapFn<S,T> avroToDerivedFn;
  private final AvroType<S> avroBaseType;

  public AvroDerivedValueDeepCopier(MapFn<T,S> derivedToAvroFn, MapFn<S,T> avroToDerivedFn, AvroType<S> avroBaseType) {
    this.derivedToAvroFn = derivedToAvroFn;
    this.avroToDerivedFn = avroToDerivedFn;
    this.avroBaseType = avroBaseType;
  }

  @Override
  public void initialize(Configuration conf) {
    avroBaseType.initialize(conf);
  }

  @Override
  public Object deepCopy(Object source) {
    return avroToDerivedFn.map(avroBaseType.getDetachedValue(derivedToAvroFn.map((T) source)));
  }
}
