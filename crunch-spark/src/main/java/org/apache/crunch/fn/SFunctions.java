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

import java.util.Iterator;

import org.apache.spark.api.java.function.DoubleFlatMapFunction;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Utility methods for wrapping existing Spark Java API Functions for
 * Crunch compatibility.
 */
public final class SFunctions {

  public static <T, R> SFunction<T, R> wrap(final Function<T, R> f) {
    return new SFunction<T, R>() {
      @Override
      public R call(T t) throws Exception {
        return f.call(t);
      }
    };
  }

  public static <K, V, R> SFunction2<K, V, R> wrap(final Function2<K, V, R> f) {
    return new SFunction2<K, V, R>() {
      @Override
      public R call(K k, V v) throws Exception {
        return f.call(k, v);
      }
    };
  }

  public static <T, K, V> SPairFunction<T, K, V> wrap(final PairFunction<T, K, V> f) {
    return new SPairFunction<T, K, V>() {
      @Override
      public Tuple2<K, V> call(T t) throws Exception {
        return f.call(t);
      }
    };
  }

  public static <T, R> SFlatMapFunction<T, R> wrap(final FlatMapFunction<T, R> f) {
    return new SFlatMapFunction<T, R>() {
      @Override
      public Iterator<R> call(T t) throws Exception {
        return f.call(t);
      }
    };
  }

  public static <K, V, R> SFlatMapFunction2<K, V, R> wrap(final FlatMapFunction2<K, V, R> f) {
    return new SFlatMapFunction2<K, V, R>() {
      @Override
      public Iterator<R> call(K k, V v) throws Exception {
        return f.call(k, v);
      }
    };
  }

  public static <T> SDoubleFunction<T> wrap(final DoubleFunction<T> f) {
    return new SDoubleFunction<T>() {
      @Override
      public double call(T t) throws Exception {
        return f.call(t);
      }
    };
  }

  public static <T> SDoubleFlatMapFunction<T> wrap(final DoubleFlatMapFunction<T> f) {
    return new SDoubleFlatMapFunction<T>() {
      @Override
      public Iterator<Double> call(T t) throws Exception {
        return f.call(t);
      }
    };
  }

  private SFunctions() {}
}
