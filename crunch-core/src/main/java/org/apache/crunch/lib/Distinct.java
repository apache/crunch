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

import java.util.Set;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * Functions for computing the distinct elements of a {@code PCollection}.
 */
public final class Distinct {

  private static final int DEFAULT_FLUSH_EVERY = 50000;
  
  /**
   * Construct a new {@code PCollection} that contains the unique elements of a
   * given input {@code PCollection}.
   * 
   * @param input The input {@code PCollection}
   * @return A new {@code PCollection} that contains the unique elements of the input
   */
  public static <S> PCollection<S> distinct(PCollection<S> input) {
    return distinct(input, DEFAULT_FLUSH_EVERY, 0);
  }
  
  /**
   * A {@code PTable<K, V>} analogue of the {@code distinct} function.
   */
  public static <K, V> PTable<K, V> distinct(PTable<K, V> input) {
    return PTables.asPTable(distinct((PCollection<Pair<K, V>>) input));
  }
  
  /**
   * A {@code distinct} operation that gives the client more control over how frequently
   * elements are flushed to disk in order to allow control over performance or
   * memory consumption.
   * 
   * @param input The input {@code PCollection}
   * @param flushEvery Flush the elements to disk whenever we encounter this many unique values
   * @return A new {@code PCollection} that contains the unique elements of the input
   */
  public static <S> PCollection<S> distinct(PCollection<S> input, int flushEvery) {
    return distinct(input, flushEvery, 0);
  }

  /**
   * A {@code PTable<K, V>} analogue of the {@code distinct} function.
   */
  public static <K, V> PTable<K, V> distinct(PTable<K, V> input, int flushEvery) {
    return PTables.asPTable(distinct((PCollection<Pair<K, V>>) input, flushEvery));
  }

    /**
   * A {@code distinct} operation that gives the client more control over how frequently
   * elements are flushed to disk in order to allow control over performance or
   * memory consumption.
   *
   * @param input       The input {@code PCollection}
   * @param flushEvery  Flush the elements to disk whenever we encounter this many unique values
   * @param numReducers The number of reducers to use
   * @return A new {@code PCollection} that contains the unique elements of the input
   */
  public static <S> PCollection<S> distinct(PCollection<S> input, int flushEvery, int numReducers) {
    Preconditions.checkArgument(flushEvery > 0);
    PType<S> pt = input.getPType();
    PTypeFamily ptf = pt.getFamily();
    return input
        .parallelDo("pre-distinct", new PreDistinctFn<S>(flushEvery, pt), ptf.tableOf(pt, ptf.nulls()))
        .groupByKey(numReducers)
        .parallelDo("post-distinct", new PostDistinctFn<S>(), pt);
  }
   /**
   * A {@code PTable<K, V>} analogue of the {@code distinct} function.
   */
  public static <K, V> PTable<K, V> distinct(PTable<K, V> input, int flushEvery, int numReducers) {
    return PTables.asPTable(distinct((PCollection<Pair<K, V>>) input, flushEvery, numReducers));
  }

  private static class PreDistinctFn<S> extends DoFn<S, Pair<S, Void>> {
    private final Set<S> values = Sets.newHashSet();
    private final int flushEvery;
    private final PType<S> ptype;
    
    PreDistinctFn(int flushEvery, PType<S> ptype) {
      this.flushEvery = flushEvery;
      this.ptype = ptype;
    }
    
    @Override
    public void initialize() {
      super.initialize();
      ptype.initialize(getConfiguration());
    }
    
    @Override
    public void process(S input, Emitter<Pair<S, Void>> emitter) {
      values.add(ptype.getDetachedValue(input));
      if (values.size() > flushEvery) {
        cleanup(emitter);
      }
    }
    
    @Override
    public void cleanup(Emitter<Pair<S, Void>> emitter) {
      for (S in : values) {
        emitter.emit(Pair.<S, Void>of(in, null));
      }
      values.clear();
    }
  }
  
  private static class PostDistinctFn<S> extends DoFn<Pair<S, Iterable<Void>>, S> {
    @Override
    public void process(Pair<S, Iterable<Void>> input, Emitter<S> emitter) {
      emitter.emit(input.first());
    }
  }
  
  // No instantiation
  private Distinct() {}
}
