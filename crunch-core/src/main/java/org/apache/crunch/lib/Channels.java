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

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.lib.Channels.FirstEmittingDoFn.SecondEmittingDoFn;
import org.apache.crunch.types.PType;

/**
 * Utilities for splitting {@link Pair} instances emitted by {@link DoFn} into
 * separate {@link PCollection} instances. A typical motivation for this might
 * be to separate standard output from error output of a DoFn.
 * 
 * @author Brandon Inman
 * 
 */
public class Channels {

  /**
   * Splits a {@link PCollection} of any {@link Pair} of objects into a Pair of
   * PCollection}, to allow for the output of a DoFn to be handled using
   * separate channels.
   *
   * @param pCollection The {@code PCollection} to split
  */
  public static <T, U> Pair<PCollection<T>, PCollection<U>> split(PCollection<Pair<T, U>> pCollection) {
    PType<Pair<T, U>> pt = pCollection.getPType();
    return split(pCollection, pt.getSubTypes().get(0), pt.getSubTypes().get(1));
  }

  /**
   * Splits a {@link PCollection} of any {@link Pair} of objects into a Pair of
   * PCollection}, to allow for the output of a DoFn to be handled using
   * separate channels.
   * 
   * @param pCollection The {@code PCollection} to split
   * @param firstPType The {@code PType} for the first collection
   * @param secondPType The {@code PType} for the second collection
   * @return {@link Pair} of {@link PCollection}
   */
  public static <T, U> Pair<PCollection<T>, PCollection<U>> split(PCollection<Pair<T, U>> pCollection,
      PType<T> firstPType, PType<U> secondPType) {

    PCollection<T> first = pCollection.parallelDo("Extract first value", new FirstEmittingDoFn<T, U>(), firstPType);
    PCollection<U> second = pCollection.parallelDo("Extract second value", new SecondEmittingDoFn<T, U>(), secondPType);
    return Pair.of(first, second);
  }

  /**
   * DoFn that emits non-null first values in a {@link Pair}.
   *
   * @author Brandon Inman
   * @param <T>
   * @param <U>
   */
   static class FirstEmittingDoFn<T, U> extends DoFn<Pair<T, U>, T> {

    @Override
    public void process(Pair<T, U> input, Emitter<T> emitter) {
      T first = input.first();
      if (first != null) {
        emitter.emit(first);
      }
    }

    /**
     * DoFn that emits non-null second values in a {@link Pair}.
     * 
     * @author Brandon Inman
     * @param <T>
     * @param <U>
     */
    static class SecondEmittingDoFn<T, U> extends DoFn<Pair<T, U>, U> {

      @Override
      public void process(Pair<T, U> input, Emitter<U> emitter) {
        U second = input.second();
        if (second != null) {
          emitter.emit(second);
        }
      }
    }
  }
}
