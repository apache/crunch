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

import org.apache.crunch.types.Converter;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class OutputConverterFunction<K, V, S> extends PairFunction<S, K, V> {
  private Converter<K, V, S, ?> converter;

  public OutputConverterFunction(Converter<K, V, S, ?> converter) {
    this.converter = converter;
  }

  @Override
  public Tuple2<K, V> call(S s) throws Exception {
    return new Tuple2<K, V>(converter.outputKey(s), converter.outputValue(s));
  }
}
