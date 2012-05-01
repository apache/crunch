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
package com.cloudera.crunch.lib.join;

import java.util.List;

import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.Pair;
import com.google.common.collect.Lists;

/**
 * Used to perform the last step of an full outer join.
 *
 * @param <K> Type of the keys.
 * @param <U> Type of the first {@link com.cloudera.crunch.PTable}'s values
 * @param <V> Type of the second {@link com.cloudera.crunch.PTable}'s values
 */
public class FullOuterJoinFn<K, U, V> extends JoinFn<K, U, V> {
  private transient int lastId;
  private transient K lastKey;
  private transient List<U> leftValues;

  /** {@inheritDoc} */
  @Override
  public void initialize() {
    lastId = 1;
    lastKey = null;
    this.leftValues = Lists.newArrayList();
  }

  /** {@inheritDoc} */
  @Override
  public void join(K key, int id, Iterable<Pair<U, V>> pairs,
      Emitter<Pair<K, Pair<U, V>>> emitter) {
    if (!key.equals(lastKey)) {
      // Make sure that left side gets emitted.
      if (0 == lastId && 0 == id) {
        for (U u : leftValues) {
          emitter.emit(Pair.of(lastKey, Pair.of(u, (V) null)));
        }
      }
      lastKey = key;
      leftValues.clear();
    }
    if (id == 0) {
      for (Pair<U, V> pair : pairs) {
        if (pair.first() != null)
          leftValues.add(pair.first());
      }
    } else {
      for (Pair<U, V> pair : pairs) {
        // Make sure that right side gets emitted.
        if (leftValues.isEmpty()) {
          leftValues.add(null);
        }
        for (U u : leftValues) {
          emitter.emit(Pair.of(lastKey, Pair.of(u, pair.second())));
        }
      }
    }

    lastId = id;
  }

  /** {@inheritDoc} */
  @Override
  public void cleanup(Emitter<Pair<K, Pair<U, V>>> emitter) {
    if (0 == lastId) {
      for (U u : leftValues) {
        emitter.emit(Pair.of(lastKey, Pair.of(u, (V) null)));
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public String getJoinType() { return "fullOuterJoin"; }
}
