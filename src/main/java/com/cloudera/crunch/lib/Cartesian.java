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
package com.cloudera.crunch.lib;

import java.util.Collection;
import java.util.Random;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.types.PTypeFamily;
import com.cloudera.crunch.types.PTableType;

/**
 * Utilities for Cartesian products of two {@code PTable} or {@code PCollection} instances.
 */
@SuppressWarnings("serial")
public class Cartesian {

  /**
   * Helper for building the artificial cross keys. This technique was taken from Pig's CROSS.
   */
  private static class GFCross<V> extends DoFn<V, Pair<Pair<Integer, Integer>, V>>{

    private final int constantField;
    private final int parallelism;
    private final Random r;

    public GFCross(int constantField, int parallelism) {
      this.constantField = constantField;
      this.parallelism = parallelism;
      this.r = new Random();
    }

    public void process(V input, Emitter<Pair<Pair<Integer, Integer>, V>> emitter) {
      int c = r.nextInt(parallelism);
      if (constantField == 0) {
        for (int i = 0; i < parallelism; i++) {
          emitter.emit(Pair.of(Pair.of(c, i), input));
        }
      } else {
        for (int i = 0; i < parallelism; i++) {
          emitter.emit(Pair.of(Pair.of(i, c), input));
        }
      }
    }
  }

  static final int DEFAULT_PARALLELISM = 6;

  /**
   * Performs a full cross join on the specified {@link PTable}s (using the same strategy as Pig's CROSS operator).
   *
   * @see <a href="http://en.wikipedia.org/wiki/Join_(SQL)#Cross_join">Cross Join</a>
   * @param left A PTable to perform a cross join on.
   * @param right A PTable to perform a cross join on.
   * @param <K1> Type of left PTable's keys.
   * @param <K2> Type of right PTable's keys.
   * @param <U> Type of the first {@link PTable}'s values
   * @param <V> Type of the second {@link PTable}'s values
   * @return The joined result as tuples of ((K1,K2), (U,V)).
   */
  public static <K1, K2, U, V> PTable<Pair<K1, K2>, Pair<U, V>> cross(
      PTable<K1, U> left,
      PTable<K2, V> right) {
    return cross(left, right, DEFAULT_PARALLELISM);
  }

  /**
   * Performs a full cross join on the specified {@link PTable}s (using the same strategy as Pig's CROSS operator).
   *
   * @see <a href="http://en.wikipedia.org/wiki/Join_(SQL)#Cross_join">Cross Join</a>
   * @param left A PTable to perform a cross join on.
   * @param right A PTable to perform a cross join on.
   * @param parallelism The square root of the number of reducers to use.  Increasing parallelism also increases copied data.
   * @param <K1> Type of left PTable's keys.
   * @param <K2> Type of right PTable's keys.
   * @param <U> Type of the first {@link PTable}'s values
   * @param <V> Type of the second {@link PTable}'s values
   * @return The joined result as tuples of ((K1,K2), (U,V)).
   */
  public static <K1, K2, U, V> PTable<Pair<K1, K2>, Pair<U, V>> cross(
      PTable<K1, U> left,
      PTable<K2, V> right,
      int parallelism) {

    /* The strategy here is to simply emulate the following PigLatin:
     *   A  = foreach table1 generate flatten(GFCross(0, 2)), flatten(*); 
     *   B  = foreach table2 generate flatten(GFCross(1, 2)), flatten(*); 
     *   C = cogroup A by ($0, $1), B by ($0, $1);
     *   result = foreach C generate flatten(A), flatten(B);
     */

    PTypeFamily ltf = left.getTypeFamily();
    PTypeFamily rtf = right.getTypeFamily();

    PTable<Pair<Integer, Integer>, Pair<K1,U>> leftCross =
        left.parallelDo(
            new GFCross<Pair<K1,U>>(0, parallelism), 
            ltf.tableOf(
                ltf.pairs(ltf.ints(), ltf.ints()), 
                ltf.pairs(left.getKeyType(), left.getValueType())));
    PTable<Pair<Integer, Integer>, Pair<K2,V>> rightCross =
        right.parallelDo(
            new GFCross<Pair<K2,V>>(1, parallelism), 
            rtf.tableOf(
                rtf.pairs(rtf.ints(), rtf.ints()), 
                rtf.pairs(right.getKeyType(), right.getValueType())));

    PTable<Pair<Integer, Integer>, Pair<Collection<Pair<K1, U>>, Collection<Pair<K2, V>>>> cg =
        leftCross.cogroup(rightCross);

    PTypeFamily ctf = cg.getTypeFamily();

    return cg.parallelDo(
        new DoFn<Pair<Pair<Integer, Integer>, Pair<Collection<Pair<K1, U>>, Collection<Pair<K2, V>>>>, Pair<Pair<K1, K2>, Pair<U, V>>>() {
          @Override
          public void process(
              Pair<Pair<Integer, Integer>, Pair<Collection<Pair<K1, U>>, Collection<Pair<K2, V>>>> input,
              Emitter<Pair<Pair<K1, K2>, Pair<U, V>>> emitter) {
            for (Pair<K1, U> l: input.second().first()) {
              for (Pair<K2, V> r: input.second().second()) {
                emitter.emit(Pair.of(Pair.of(l.first(), r.first()), Pair.of(l.second(), r.second())));
              }
            }
          }
        },
        ctf.tableOf(
            ctf.pairs(left.getKeyType(), right.getKeyType()), 
            ctf.pairs(left.getValueType(), right.getValueType()))
        );
  }

  /**
   * Performs a full cross join on the specified {@link PCollection}s (using the same strategy as Pig's CROSS operator).
   *
   * @see <a href="http://en.wikipedia.org/wiki/Join_(SQL)#Cross_join">Cross Join</a>
   * @param left A PCollection to perform a cross join on.
   * @param right A PCollection to perform a cross join on.
   * @param <U> Type of the first {@link PCollection}'s values
   * @param <V> Type of the second {@link PCollection}'s values
   * @return The joined result as tuples of (U,V).
   */
  public static <U, V> PCollection<Pair<U, V>> cross(
      PCollection<U> left,
      PCollection<V> right) {
    return cross(left, right, DEFAULT_PARALLELISM);
  }

  /**
   * Performs a full cross join on the specified {@link PCollection}s (using the same strategy as Pig's CROSS operator).
   *
   * @see <a href="http://en.wikipedia.org/wiki/Join_(SQL)#Cross_join">Cross Join</a>
   * @param left A PCollection to perform a cross join on.
   * @param right A PCollection to perform a cross join on.
   * @param <U> Type of the first {@link PCollection}'s values
   * @param <V> Type of the second {@link PCollection}'s values
   * @return The joined result as tuples of (U,V).
   */
  public static <U, V> PCollection<Pair<U, V>> cross(
      PCollection<U> left,
      PCollection<V> right,
      int parallelism) {

    PTypeFamily ltf = left.getTypeFamily();
    PTypeFamily rtf = right.getTypeFamily();

    PTableType<Pair<Integer, Integer>, U> ptt = ltf.tableOf(
        ltf.pairs(ltf.ints(), ltf.ints()), 
        left.getPType());

    if (ptt == null)
      throw new Error();

    PTable<Pair<Integer, Integer>, U> leftCross =
        left.parallelDo(
            new GFCross<U>(0, parallelism), 
            ltf.tableOf(
                ltf.pairs(ltf.ints(), ltf.ints()), 
                left.getPType()));
    PTable<Pair<Integer, Integer>, V> rightCross =
        right.parallelDo(
            new GFCross<V>(1, parallelism), 
            rtf.tableOf(
                rtf.pairs(rtf.ints(), rtf.ints()), 
                right.getPType()));

    PTable<Pair<Integer, Integer>, Pair<Collection<U>, Collection<V>>> cg =
        leftCross.cogroup(rightCross);

    PTypeFamily ctf = cg.getTypeFamily();

    return cg.parallelDo(
        new DoFn<Pair<Pair<Integer, Integer>, Pair<Collection<U>, Collection<V>>>, Pair<U,V>>() {
          @Override
          public void process(
              Pair<Pair<Integer, Integer>, Pair<Collection<U>, Collection<V>>> input,
              Emitter<Pair<U,V>> emitter) {
            for (U l: input.second().first()) {
              for (V r: input.second().second()) {
                emitter.emit(Pair.of(l, r));
              }
            }
          }
        }, ctf.pairs(left.getPType(), right.getPType()));
  }
  
}
