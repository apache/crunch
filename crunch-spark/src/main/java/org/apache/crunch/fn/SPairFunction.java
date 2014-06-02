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
import org.apache.crunch.Pair;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * A Crunch-compatible abstract base class for Spark's {@link PairFunction}. Subclasses
 * of this class may be used against either Crunch {@code PCollections} or Spark {@code RDDs}.
 */
public abstract class SPairFunction<T, K, V> extends SparkMapFn<T, Pair<K, V>>
    implements PairFunction<T, K, V> {
  @Override
  public Pair<K, V> map(T input) {
    try {
      Tuple2<K, V> t = call(input);
      return t == null ? null : Pair.of(t._1(), t._2());
    } catch (Exception e) {
      throw new CrunchRuntimeException(e);
    }
  }
}
