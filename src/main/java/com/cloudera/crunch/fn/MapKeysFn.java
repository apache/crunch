/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.crunch.fn;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.Pair;

public abstract class MapKeysFn<K1, K2, V> extends DoFn<Pair<K1, V>, Pair<K2, V>> {
  @Override
  public void process(Pair<K1, V> input, Emitter<Pair<K2, V>> emitter) {
    emitter.emit(Pair.of(map(input.first()), input.second()));
  }

  public abstract K2 map(K1 k1);
}
