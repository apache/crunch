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

import java.util.Random;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;

import com.google.common.base.Preconditions;

public class Sample {

  private static class SamplerFn<S> extends DoFn<S, S> {

    private final long seed;
    private final double acceptanceProbability;
    private transient Random r;

    public SamplerFn(long seed, double acceptanceProbability) {
      Preconditions.checkArgument(0.0 < acceptanceProbability && acceptanceProbability < 1.0);
      this.seed = seed;
      this.acceptanceProbability = acceptanceProbability;
    }

    @Override
    public void initialize() {
      r = new Random(seed);
    }

    @Override
    public void process(S input, Emitter<S> emitter) {
      if (r.nextDouble() < acceptanceProbability) {
        emitter.emit(input);
      }
    }
  }

  public static <S> PCollection<S> sample(PCollection<S> input, double probability) {
    return sample(input, System.currentTimeMillis(), probability);
  }

  public static <S> PCollection<S> sample(PCollection<S> input, long seed, double probability) {
    String stageName = String.format("sample(%.2f)", probability);
    return input.parallelDo(stageName, new SamplerFn<S>(seed, probability), input.getPType());
  }
}
