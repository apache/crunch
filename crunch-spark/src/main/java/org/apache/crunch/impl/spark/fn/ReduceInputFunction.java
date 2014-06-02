/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.apache.crunch.impl.spark.fn;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.spark.ByteArray;
import org.apache.crunch.impl.spark.serde.SerDe;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.List;

public class ReduceInputFunction<K, V> implements Function<Tuple2<ByteArray, Iterable<byte[]>>, Pair<K, Iterable<V>>> {
  private final SerDe<K> keySerDe;
  private final SerDe<V> valueSerDe;

  public ReduceInputFunction(SerDe<K> keySerDe, SerDe<V> valueSerDe) {
    this.keySerDe = keySerDe;
    this.valueSerDe = valueSerDe;
  }

  @Override
  public Pair<K, Iterable<V>> call(Tuple2<ByteArray, Iterable<byte[]>> kv) throws Exception {
    return Pair.of(keySerDe.fromBytes(kv._1().value), Iterables.transform(kv._2(), valueSerDe.fromBytesFunction()));
  }
}
