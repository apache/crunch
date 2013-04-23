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

import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.ParallelDoOptions;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.io.ReadableSourceTarget;
import org.apache.crunch.materialize.MaterializableIterable;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.util.DistCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * Utility for doing map side joins on a common key between two {@link PTable}s.
 * <p>
 * A map side join is an optimized join which doesn't use a reducer; instead,
 * the right side of the join is loaded into memory and the join is performed in
 * a mapper. This style of join has the important implication that the output of
 * the join is not sorted, which is the case with a conventional (reducer-based)
 * join.
 * <p>
 * <b>Note:</b>This utility is only supported when running with a
 * {@link MRPipeline} as the pipeline.
 */
public class MapsideJoin {

  /**
   * Join two tables using a map side join. The right-side table will be loaded
   * fully in memory, so this method should only be used if the right side
   * table's contents can fit in the memory allocated to mappers. The join
   * performed by this method is an inner join.
   * 
   * @param left
   *          The left-side table of the join
   * @param right
   *          The right-side table of the join, whose contents will be fully
   *          read into memory
   * @return A table keyed on the join key, containing pairs of joined values
   */
  public static <K, U, V> PTable<K, Pair<U, V>> join(PTable<K, U> left, PTable<K, V> right) {
    PTypeFamily tf = left.getTypeFamily();
    Iterable<Pair<K, V>> iterable = right.materialize();

    if (iterable instanceof MaterializableIterable) {
      MaterializableIterable<Pair<K, V>> mi = (MaterializableIterable<Pair<K, V>>) iterable;
      MapsideJoinDoFn<K, U, V> mapJoinDoFn = new MapsideJoinDoFn<K, U, V>(mi.getPath().toString(),
          right.getPType());
      ParallelDoOptions.Builder optionsBuilder = ParallelDoOptions.builder();
      if (mi.isSourceTarget()) {
        optionsBuilder.sourceTargets((SourceTarget) mi.getSource());
      }
      return left.parallelDo("mapjoin", mapJoinDoFn,
          tf.tableOf(left.getKeyType(), tf.pairs(left.getValueType(), right.getValueType())),
          optionsBuilder.build());
    } else { // in-memory pipeline
      return left.parallelDo(new InMemoryJoinFn<K, U, V>(iterable),
          tf.tableOf(left.getKeyType(), tf.pairs(left.getValueType(), right.getValueType())));
    }
  }

  static class InMemoryJoinFn<K, U, V> extends DoFn<Pair<K, U>, Pair<K, Pair<U, V>>> {

    private Multimap<K, V> joinMap;
    
    public InMemoryJoinFn(Iterable<Pair<K, V>> iterable) {
      joinMap = HashMultimap.create();
      for (Pair<K, V> joinPair : iterable) {
        joinMap.put(joinPair.first(), joinPair.second());
      }
    }
    
    @Override
    public void process(Pair<K, U> input, Emitter<Pair<K, Pair<U, V>>> emitter) {
      K key = input.first();
      U value = input.second();
      for (V joinValue : joinMap.get(key)) {
        Pair<U, V> valuePair = Pair.of(value, joinValue);
        emitter.emit(Pair.of(key, valuePair));
      }
    }
  }
  
  static class MapsideJoinDoFn<K, U, V> extends DoFn<Pair<K, U>, Pair<K, Pair<U, V>>> {

    private String inputPath;
    private PType<Pair<K, V>> ptype;
    private Multimap<K, V> joinMap;

    public MapsideJoinDoFn(String inputPath, PType<Pair<K, V>> ptype) {
      this.inputPath = inputPath;
      this.ptype = ptype;
    }

    private Path getCacheFilePath() {
      Path local = DistCache.getPathToCacheFile(new Path(inputPath), getConfiguration());
      if (local == null) {
        throw new CrunchRuntimeException("Can't find local cache file for '" + inputPath + "'");
      }
      return local;
    }

    @Override
    public void configure(Configuration conf) {
      DistCache.addCacheFile(new Path(inputPath), conf);
    }
    
    @Override
    public void initialize() {
      super.initialize();

      ReadableSourceTarget<Pair<K, V>> sourceTarget = ptype.getDefaultFileSource(
          getCacheFilePath());
      Iterable<Pair<K, V>> iterable = null;
      try {
        iterable = sourceTarget.read(getConfiguration());
      } catch (IOException e) {
        throw new CrunchRuntimeException("Error reading right-side of map side join: ", e);
      }

      joinMap = ArrayListMultimap.create();
      for (Pair<K, V> joinPair : iterable) {
        joinMap.put(joinPair.first(), joinPair.second());
      }
    }

    @Override
    public void process(Pair<K, U> input, Emitter<Pair<K, Pair<U, V>>> emitter) {
      K key = input.first();
      U value = input.second();
      for (V joinValue : joinMap.get(key)) {
        Pair<U, V> valuePair = Pair.of(value, joinValue);
        emitter.emit(Pair.of(key, valuePair));
      }
    }
  }
}
