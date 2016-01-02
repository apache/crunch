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
package org.apache.crunch.lambda;

import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.lambda.fn.SBiFunction;
import org.apache.crunch.lambda.fn.SFunction;
import org.apache.crunch.lambda.fn.SSupplier;

/**
 * Crunch Aggregator expressed as a composition of functional interface implementations
 * @param <V> Type of values to be aggregated
 * @param <A> Type of object which stores objects as they are being aggregated
 */
public class LAggregator<V, A> extends Aggregators.SimpleAggregator<V> {

    private final SSupplier<A> initialSupplier;
    private final SBiFunction<A, V, A> combineFn;
    private final SFunction<A, Iterable<V>> outputFn;
    private A a;

    public LAggregator(SSupplier<A> initialSupplier, SBiFunction<A, V, A> combineFn, SFunction<A, Iterable<V>> outputFn) {
        this.initialSupplier = initialSupplier;
        this.combineFn = combineFn;
        this.outputFn = outputFn;
    }

    @Override
    public void reset() {
        a = initialSupplier.get();
    }

    @Override
    public void update(V v) {
        a = combineFn.apply(a, v);
    }

    @Override
    public Iterable<V> results() {
        return outputFn.apply(a);
    }
}
