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

import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;

/**
 * Swap the elements of a {@code Pair} type.
 */
public class SwapFn<V1, V2> extends MapFn<Pair<V1, V2>, Pair<V2, V1>> {

  public static <V1, V2> PType<Pair<V2, V1>> ptype(PType<Pair<V1, V2>> pt) {
    return pt.getFamily().pairs(pt.getSubTypes().get(1), pt.getSubTypes().get(0));
  }

  public static <K, V> PTableType<V, K> tableType(PTableType<K, V> ptt) {
    return ptt.getFamily().tableOf(ptt.getValueType(), ptt.getKeyType());
  }

  @Override
  public Pair<V2, V1> map(Pair<V1, V2> input) {
    if (input == null) {
      return null;
    }
    return Pair.of(input.second(), input.first());
  }
}
