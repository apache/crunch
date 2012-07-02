package com.cloudera.crunch.lib.join;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.impl.mr.run.CrunchRuntimeException;
import com.cloudera.crunch.io.ReadableSourceTarget;
import com.cloudera.crunch.io.impl.SourcePathTargetImpl;
import com.cloudera.crunch.types.PType;
import com.cloudera.crunch.types.PTypeFamily;
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

    // TODO Make this method internal to MRPipeline so that we don't run once
    // for every separate MapsideJoin at the same level
    pipeline.run();

    // TODO Verify that this cast is safe -- are there any situations where this
    // wouldn't work?
    SourcePathTargetImpl<Pair<K, V>> sourcePathTarget = (SourcePathTargetImpl<Pair<K, V>>) pipeline
        .getMaterializeSourceTarget(right);

    // TODO Put the data in the distributed cache

    Path path = sourcePathTarget.getPath();
    PType<Pair<K, V>> pType = right.getPType();

    MapsideJoinDoFn<K, U, V> mapJoinDoFn = new MapsideJoinDoFn<K, U, V>(path.toString(), pType);
    PTypeFamily typeFamily = left.getTypeFamily();
    return left.parallelDo(
        "mapjoin",
        mapJoinDoFn,
        typeFamily.tableOf(left.getKeyType(),
            typeFamily.pairs(left.getValueType(), right.getValueType())));

  }

  static class MapsideJoinDoFn<K, U, V> extends DoFn<Pair<K, U>, Pair<K, Pair<U, V>>> {

    private String path;
    private PType<Pair<K, V>> ptype;
    private Multimap<K, V> joinMap;

    public MapsideJoinDoFn(String path, PType<Pair<K, V>> ptype) {
      this.path = path;
      this.ptype = ptype;
    }

    @Override
    public void initialize() {
      super.initialize();

      ReadableSourceTarget<Pair<K, V>> sourceTarget = (ReadableSourceTarget<Pair<K, V>>) ptype
          .getDefaultFileSource(new Path(path));
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
