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
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.ReadableSourceTarget;
import org.apache.crunch.io.impl.SourcePathTargetImpl;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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

    if (!(right.getPipeline() instanceof MRPipeline)) {
      throw new CrunchRuntimeException("Map-side join is only supported within a MapReduce context");
    }

    MRPipeline pipeline = (MRPipeline) right.getPipeline();
    pipeline.materialize(right);

    // TODO Move necessary logic to MRPipeline so that we can theoretically
    // optimize his by running the setup of multiple map-side joins concurrently
    pipeline.run();

    ReadableSourceTarget<Pair<K, V>> readableSourceTarget = pipeline.getMaterializeSourceTarget(right);
    if (!(readableSourceTarget instanceof SourcePathTargetImpl)) {
      throw new CrunchRuntimeException("Right-side contents can't be read from a path");
    }

    // Suppress warnings because we've just checked this cast via instanceof
    @SuppressWarnings("unchecked")
    SourcePathTargetImpl<Pair<K, V>> sourcePathTarget = (SourcePathTargetImpl<Pair<K, V>>) readableSourceTarget;

    Path path = sourcePathTarget.getPath();
    DistributedCache.addCacheFile(path.toUri(), pipeline.getConfiguration());

    MapsideJoinDoFn<K, U, V> mapJoinDoFn = new MapsideJoinDoFn<K, U, V>(path.getName(), right.getPType());
    PTypeFamily typeFamily = left.getTypeFamily();
    return left.parallelDo("mapjoin", mapJoinDoFn,
        typeFamily.tableOf(left.getKeyType(), typeFamily.pairs(left.getValueType(), right.getValueType())));

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
      try {
        for (Path localPath : DistributedCache.getLocalCacheFiles(getConfiguration())) {
          if (localPath.toString().endsWith(inputPath)) {
            return localPath.makeQualified(FileSystem.getLocal(getConfiguration()));

          }
        }
      } catch (IOException e) {
        throw new CrunchRuntimeException(e);
      }

      throw new CrunchRuntimeException("Can't find local cache file for '" + inputPath + "'");
    }

    @Override
    public void initialize() {
      super.initialize();

      ReadableSourceTarget<Pair<K, V>> sourceTarget = (ReadableSourceTarget<Pair<K, V>>) ptype
          .getDefaultFileSource(getCacheFilePath());
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
