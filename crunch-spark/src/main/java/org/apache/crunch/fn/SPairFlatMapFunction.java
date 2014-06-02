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
package org.apache.crunch.fn;

import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

/**
 * A Crunch-compatible abstract base class for Spark's {@link PairFlatMapFunction}. Subclasses
 * of this class may be used against either Crunch {@code PCollections} or Spark {@code RDDs}.
 */
public abstract class SPairFlatMapFunction<T, K, V> extends SparkDoFn<T, Pair<K, V>>
    implements PairFlatMapFunction<T, K, V> {
  @Override
  public void process(T input, Emitter<Pair<K, V>> emitter) {
    try {
      for (Tuple2<K, V> kv : call(input)) {
        emitter.emit(Pair.of(kv._1(), kv._2()));
      }
    } catch (Exception e) {
      throw new CrunchRuntimeException(e);
    }
  }

}
