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
package org.apache.crunch.lib;

import java.util.Collection;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Tuple3;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;

/**
 * Utilities for performing set operations (difference, intersection, etc) on
 * {@code PCollection} instances.
 */
public class Set {

  /**
   * Compute the set difference between two sets of elements.
   * 
   * @return a collection containing elements that are in <code>coll1</code> but
   *         not in <code>coll2</code>
   */
  public static <T> PCollection<T> difference(PCollection<T> coll1, PCollection<T> coll2) {
    return Cogroup.cogroup(toTable(coll1), toTable(coll2)).parallelDo(
        "Calculate differences of sets",
        new DoFn<Pair<T, Pair<Collection<Boolean>, Collection<Boolean>>>, T>() {
          @Override
          public void process(Pair<T, Pair<Collection<Boolean>, Collection<Boolean>>> input, Emitter<T> emitter) {
            Pair<Collection<Boolean>, Collection<Boolean>> groups = input.second();
            if (!groups.first().isEmpty() && groups.second().isEmpty()) {
              emitter.emit(input.first());
            }
          }
        }, coll1.getPType());
  }

  /**
   * Compute the intersection of two sets of elements.
   * 
   * @return a collection containing elements that common to both sets
   *         <code>coll1</code> and <code>coll2</code>
   */
  public static <T> PCollection<T> intersection(PCollection<T> coll1, PCollection<T> coll2) {
    return Cogroup.cogroup(toTable(coll1), toTable(coll2)).parallelDo(
        "Calculate intersection of sets",
        new DoFn<Pair<T, Pair<Collection<Boolean>, Collection<Boolean>>>, T>() {
          @Override
          public void process(Pair<T, Pair<Collection<Boolean>, Collection<Boolean>>> input, Emitter<T> emitter) {
            Pair<Collection<Boolean>, Collection<Boolean>> groups = input.second();
            if (!groups.first().isEmpty() && !groups.second().isEmpty()) {
              emitter.emit(input.first());
            }
          }
        }, coll1.getPType());
  }

  /**
   * Find the elements that are common to two sets, like the Unix
   * <code>comm</code> utility. This method returns a {@link PCollection} of
   * {@link Tuple3} objects, and the position in the tuple that an element
   * appears is determined by the collections that it is a member of, as
   * follows:
   * <ol>
   * <li>elements only in <code>coll1</code>,</li>
   * <li>elements only in <code>coll2</code>, or</li>
   * <li>elements in both collections</li>
   * </ol>
   * Tuples are otherwise filled with <code>null</code>.
   * 
   * @return a collection of {@link Tuple3} objects
   */
  public static <T> PCollection<Tuple3<T, T, T>> comm(PCollection<T> coll1, PCollection<T> coll2) {
    PTypeFamily typeFamily = coll1.getTypeFamily();
    PType<T> type = coll1.getPType();
    return Cogroup.cogroup(toTable(coll1), toTable(coll2)).parallelDo(
        "Calculate common values of sets",
        new DoFn<Pair<T, Pair<Collection<Boolean>, Collection<Boolean>>>, Tuple3<T, T, T>>() {
          @Override
          public void process(Pair<T, Pair<Collection<Boolean>, Collection<Boolean>>> input,
              Emitter<Tuple3<T, T, T>> emitter) {
            Pair<Collection<Boolean>, Collection<Boolean>> groups = input.second();
            boolean inFirst = !groups.first().isEmpty();
            boolean inSecond = !groups.second().isEmpty();
            T t = input.first();
            emitter.emit(Tuple3.of(inFirst && !inSecond ? t : null, !inFirst && inSecond ? t : null, inFirst
                && inSecond ? t : null));
          }
        }, typeFamily.triples(type, type, type));
  }

  private static <T> PTable<T, Boolean> toTable(PCollection<T> coll) {
    PTypeFamily typeFamily = coll.getTypeFamily();
    return coll.parallelDo(new DoFn<T, Pair<T, Boolean>>() {
      @Override
      public void process(T input, Emitter<Pair<T, Boolean>> emitter) {
        emitter.emit(Pair.of(input, Boolean.TRUE));
      }
    }, typeFamily.tableOf(coll.getPType(), typeFamily.booleans()));
  }

}
