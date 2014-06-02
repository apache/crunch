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
import org.apache.spark.api.java.function.FlatMapFunction;

/**
 * A Crunch-compatible abstract base class for Spark's {@link FlatMapFunction}. Subclasses
 * of this class may be used against either Crunch {@code PCollections} or Spark {@code RDDs}.
 */
public abstract class SFlatMapFunction<T, R> extends SparkDoFn<T, R>
    implements FlatMapFunction<T, R> {
  @Override
  public void process(T input, Emitter<R> emitter) {
    try {
      for (R r : call(input)) {
        emitter.emit(r);
      }
    } catch (Exception e) {
      throw new CrunchRuntimeException(e);
    }
  }
}
