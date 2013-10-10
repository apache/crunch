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

import java.io.IOException;
import java.util.Collection;

import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.ParallelDoOptions;
import org.apache.crunch.ReadableData;
import org.apache.crunch.types.PTypeFamily;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

/**
 * Utility for doing map side joins on a common key between two {@link PTable}s.
 * <p>
 * A map side join is an optimized join which doesn't use a reducer; instead,
 * the right side of the join is loaded into memory and the join is performed in
 * a mapper. This style of join has the important implication that the output of
 * the join is not sorted, which is the case with a conventional (reducer-based)
 * join.
 */
public class MapsideJoinStrategy<K, U, V> implements JoinStrategy<K, U, V> {

  private boolean materialize;

  /**
   * Constructs a new instance of the {@code MapsideJoinStratey}, materializing the right-side
   * join table to disk before the join is performed.
   */
  public MapsideJoinStrategy() {
    this(true);
  }

  /**
   * Constructs a new instance of the {@code MapsideJoinStrategy}. If the {@code }materialize}
   * argument is true, then the right-side join {@code PTable} will be materialized to disk
   * before the in-memory join is performed. If it is false, then Crunch can optionally read
   * and process the data from the right-side table without having to run a job to materialize
   * the data to disk first.
   *
   * @param materialize Whether or not to materialize the right-side table before the join
   */
  public MapsideJoinStrategy(boolean materialize) {
    this.materialize = materialize;
  }

  @Override
  public PTable<K, Pair<U, V>> join(PTable<K, U> left, PTable<K, V> right, JoinType joinType) {
    switch (joinType) {
    case INNER_JOIN:
      return joinInternal(left, right, false);
    case LEFT_OUTER_JOIN:
      return joinInternal(left, right, true);
    default:
      throw new UnsupportedOperationException("Join type " + joinType
          + " not supported by MapsideJoinStrategy");
    }
  }
  

  private PTable<K, Pair<U,V>> joinInternal(PTable<K, U> left, PTable<K, V> right, boolean includeUnmatchedLeftValues) {
    PTypeFamily tf = left.getTypeFamily();
    ReadableData<Pair<K, V>> rightReadable = right.asReadable(materialize);
    MapsideJoinDoFn<K, U, V> mapJoinDoFn = new MapsideJoinDoFn<K, U, V>(rightReadable, includeUnmatchedLeftValues);
    ParallelDoOptions options = ParallelDoOptions.builder()
        .sourceTargets(rightReadable.getSourceTargets())
        .build();
    return left.parallelDo("mapjoin", mapJoinDoFn,
        tf.tableOf(left.getKeyType(), tf.pairs(left.getValueType(), right.getValueType())),
        options);
  }

  static class MapsideJoinDoFn<K, U, V> extends DoFn<Pair<K, U>, Pair<K, Pair<U, V>>> {

    private final ReadableData<Pair<K, V>> readable;
    private final boolean includeUnmatched;
    private Multimap<K, V> joinMap;

    public MapsideJoinDoFn(ReadableData<Pair<K, V>> rs, boolean includeUnmatched) {
      this.readable = rs;
      this.includeUnmatched = includeUnmatched;
    }

    @Override
    public void configure(Configuration conf) {
      readable.configure(conf);
    }
    
    @Override
    public void initialize() {
      super.initialize();

      joinMap = ArrayListMultimap.create();
      try {
        for (Pair<K, V> joinPair : readable.read(getContext())) {
          joinMap.put(joinPair.first(), joinPair.second());
        }
      } catch (IOException e) {
        throw new CrunchRuntimeException("Error reading map-side join data", e);
      }
    }

    @Override
    public void process(Pair<K, U> input, Emitter<Pair<K, Pair<U, V>>> emitter) {
      K key = input.first();
      U value = input.second();
      Collection<V> joinValues = joinMap.get(key);
      if (includeUnmatched && joinValues.isEmpty()) {
        emitter.emit(Pair.of(key, Pair.<U,V>of(value, null)));
      } else {
        for (V joinValue : joinValues) {
          Pair<U, V> valuePair = Pair.of(value, joinValue);
          emitter.emit(Pair.of(key, valuePair));
        }
      }
    }
  }
}
