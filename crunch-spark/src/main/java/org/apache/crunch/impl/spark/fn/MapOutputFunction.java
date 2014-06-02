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
package org.apache.crunch.impl.spark.fn;

import org.apache.crunch.Pair;
import org.apache.crunch.impl.spark.ByteArray;
import org.apache.crunch.impl.spark.serde.SerDe;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class MapOutputFunction<K, V> implements PairFunction<Pair<K, V>, ByteArray, byte[]> {

  private final SerDe keySerde;
  private final SerDe valueSerde;

  public MapOutputFunction(SerDe keySerde, SerDe valueSerde) {
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
  }

  @Override
  public Tuple2<ByteArray, byte[]> call(Pair<K, V> p) throws Exception {
    return new Tuple2<ByteArray, byte[]>(
        new ByteArray(keySerde.toBytes(p.first())),
        valueSerde.toBytes(p.second()));
  }
}
