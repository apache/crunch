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
package org.apache.crunch.lib.join;

import java.util.List;

import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.crunch.types.PType;

import com.google.common.collect.Lists;

/**
 * Used to perform the last step of an right outer join.
 * 
 * @param <K> Type of the keys.
 * @param <U> Type of the first {@link org.apache.crunch.PTable}'s values
 * @param <V> Type of the second {@link org.apache.crunch.PTable}'s values
 */
public class RightOuterJoinFn<K, U, V> extends JoinFn<K, U, V> {

  private transient K lastKey;
  private transient List<U> leftValues;

  public RightOuterJoinFn(PType<K> keyType, PType<U> leftValueType) {
    super(keyType, leftValueType);
  }

  /** {@inheritDoc} */
  @Override
  public void initialize() {
    super.initialize();
    lastKey = null;
    this.leftValues = Lists.newArrayList();
  }

  /** {@inheritDoc} */
  @Override
  public void join(K key, int id, Iterable<Pair<U, V>> pairs, Emitter<Pair<K, Pair<U, V>>> emitter) {
    if (!key.equals(lastKey)) {
      lastKey = keyType.getDetachedValue(key);
      leftValues.clear();
    }
    if (id == 0) {
      for (Pair<U, V> pair : pairs) {
        if (pair.first() != null)
          leftValues.add(leftValueType.getDetachedValue(pair.first()));
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
  }

  /** {@inheritDoc} */
  @Override
  public String getJoinType() {
    return "rightOuterJoin";
  }
}
