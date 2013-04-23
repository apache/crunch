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

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;

import org.apache.crunch.CombineFn;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.FilterFn;
import org.apache.crunch.Pair;
import org.apache.crunch.types.PType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

class SampleUtils {
  
  static class SampleFn<S> extends FilterFn<S> {

    private final Long seed;
    private final double acceptanceProbability;
    private transient Random r;

    public SampleFn(double acceptanceProbability, Long seed) {
      Preconditions.checkArgument(0.0 < acceptanceProbability && acceptanceProbability < 1.0);
      this.seed = seed == null ? System.currentTimeMillis() : seed;
      this.acceptanceProbability = acceptanceProbability;
    }

    @Override
    public void initialize() {
      if (r == null) {
        r = new Random(seed);
      }
    }

    @Override
    public boolean accept(S input) {
      return r.nextDouble() < acceptanceProbability;
    }
  }


  static class ReservoirSampleFn<T, N extends Number>
      extends DoFn<Pair<Integer, Pair<T, N>>, Pair<Integer, Pair<Double, T>>> {
  
    private int[] sampleSizes;
    private Long seed;
    private PType<T> valueType;
    private transient List<SortedMap<Double, T>> reservoirs;
    private transient Random random;
    
    public ReservoirSampleFn(int[] sampleSizes, Long seed, PType<T> valueType) {
      this.sampleSizes = sampleSizes;
      this.seed = seed;
      this.valueType = valueType;
    }
    
    @Override
    public void initialize() {
      this.reservoirs = Lists.newArrayList();
      this.valueType.initialize(getConfiguration());
      for (int i = 0; i < sampleSizes.length; i++) {
        reservoirs.add(Maps.<Double, T>newTreeMap());
      }
      if (random == null) {
        if (seed == null) {
          this.random = new Random();
        } else {
          this.random = new Random(seed);
        }
      }
    }
    
    @Override
    public void process(Pair<Integer, Pair<T, N>> input,
        Emitter<Pair<Integer, Pair<Double, T>>> emitter) {
      int id = input.first();
      Pair<T, N> p = input.second();
      double weight = p.second().doubleValue();
      if (weight > 0.0) {
        double score = Math.log(random.nextDouble()) / weight;
        SortedMap<Double, T> reservoir = reservoirs.get(id);
        if (reservoir.size() < sampleSizes[id]) { 
          reservoir.put(score, valueType.getDetachedValue(p.first()));        
        } else if (score > reservoir.firstKey()) {
          reservoir.remove(reservoir.firstKey());
          reservoir.put(score, valueType.getDetachedValue(p.first()));
        }
      }
    }
    
    @Override
    public void cleanup(Emitter<Pair<Integer, Pair<Double, T>>> emitter) {
      for (int id = 0; id < reservoirs.size(); id++) {
        SortedMap<Double, T> reservoir = reservoirs.get(id);
        for (Map.Entry<Double, T> e : reservoir.entrySet()) {
          emitter.emit(Pair.of(id, Pair.of(e.getKey(), e.getValue())));
        }
      }
    }
  }
  
  static class WRSCombineFn<T> extends CombineFn<Integer, Pair<Double, T>> {

    private int[] sampleSizes;
    private PType<T> valueType;
    private List<SortedMap<Double, T>> reservoirs;
    
    public WRSCombineFn(int[] sampleSizes, PType<T> valueType) {
      this.sampleSizes = sampleSizes;
      this.valueType = valueType;
    }

    @Override
    public void initialize() {
      this.reservoirs = Lists.newArrayList();
      for (int i = 0; i < sampleSizes.length; i++) {
        reservoirs.add(Maps.<Double, T>newTreeMap());
      }
      this.valueType.initialize(getConfiguration());
    }
    
    @Override
    public void process(Pair<Integer, Iterable<Pair<Double, T>>> input,
        Emitter<Pair<Integer, Pair<Double, T>>> emitter) {
      SortedMap<Double, T> reservoir = reservoirs.get(input.first());
      for (Pair<Double, T> p : input.second()) {
        if (reservoir.size() < sampleSizes[input.first()]) { 
          reservoir.put(p.first(), valueType.getDetachedValue(p.second()));        
        } else if (p.first() > reservoir.firstKey()) {
          reservoir.remove(reservoir.firstKey());
          reservoir.put(p.first(), valueType.getDetachedValue(p.second()));  
        }
      }
    }
    
    @Override
    public void cleanup(Emitter<Pair<Integer, Pair<Double, T>>> emitter) {
      for (int i = 0; i < reservoirs.size(); i++) {
        SortedMap<Double, T> reservoir = reservoirs.get(i);
        for (Map.Entry<Double, T> e : reservoir.entrySet()) {
          emitter.emit(Pair.of(i, Pair.of(e.getKey(), e.getValue())));
        }
      }
    }
  }
}
