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
package org.apache.crunch.impl.spark;

import com.google.common.base.Function;
import org.apache.crunch.Pair;
import scala.Tuple2;

import javax.annotation.Nullable;

public class GuavaUtils {
  public static <K, V> Function<Tuple2<K, V>, Pair<K, V>> tuple2PairFunc() {
    return new Function<Tuple2<K, V>, Pair<K, V>>() {
      @Override
      public Pair<K, V> apply(@Nullable Tuple2<K, V> kv) {
        return kv == null ? null : Pair.of(kv._1, kv._2);
      }
    };
  }

  public static <K, V> Function<Pair<K, V>, Tuple2<K, V>> pair2tupleFunc() {
    return new Function<Pair<K, V>, Tuple2<K, V>>() {
      @Override
      public Tuple2<K, V> apply(@Nullable Pair<K, V> kv) {
        return kv == null ? null : new Tuple2<K, V>(kv.first(), kv.second());
      }
    };
  }
}
