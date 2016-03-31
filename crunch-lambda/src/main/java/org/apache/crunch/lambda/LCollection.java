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

import org.apache.crunch.*;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.lambda.fn.SFunction;
import org.apache.crunch.lambda.fn.SPredicate;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;

import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Java 8 friendly version of the {@link PCollection} interface, allowing distributed operations to be expressed in
 * terms of lambda expressions and method references, instead of creating a new class implementation for each operation.
 * @param <S> The type of the elements in this collection
 */
public interface LCollection<S> {
    /**
     * Get the underlying {@link PCollection} for this LCollection
     */
    PCollection<S> underlying();

    /**
     * Get the {@link LCollectionFactory} which can be used to create new Ltype instances
     */
    LCollectionFactory factory();

    /**
     * Transform this LCollection using a standard Crunch {@link DoFn}
     */
    default <T> LCollection<T> parallelDo(DoFn<S, T> fn, PType<T> pType) {
        return factory().wrap(underlying().parallelDo(fn, pType));
    }

    /**
     * Transform this LCollection to an {@link LTable} using a standard Crunch {@link DoFn}
     */
    default <K, V> LTable<K, V> parallelDo(DoFn<S, Pair<K, V>> fn, PTableType<K, V> pType) {
        return factory().wrap(underlying().parallelDo(fn, pType));
    }

    /**
     * Transform this LCollection using a Lambda-friendly {@link LDoFn}.
     */
    default <T> LCollection<T> parallelDo(LDoFn<S, T> fn, PType<T> pType) {
        return parallelDo(new LDoFnWrapper<>(fn), pType);
    }

    /**
     * Transform this LCollection using a Lambda-friendly {@link LDoFn}.
     */
    default <K, V> LTable<K, V> parallelDo(LDoFn<S, Pair<K, V>> fn, PTableType<K, V> pType) {
        return parallelDo(new LDoFnWrapper<>(fn), pType);
    }

    /**
     * Map the elements of this collection 1-1 through the supplied function.
     */
    default <T> LCollection<T> map(SFunction<S, T> fn, PType<T> pType) {
        return parallelDo(ctx -> ctx.emit(fn.apply(ctx.element())), pType);
    }

    /**
     * Map the elements of this collection 1-1 through the supplied function to yield an {@link LTable}
     */
    default <K, V> LTable<K, V> map(SFunction<S, Pair<K, V>> fn, PTableType<K, V> pType) {
        return parallelDo(ctx -> ctx.emit(fn.apply(ctx.element())), pType);
    }

    /**
     * Map each element to zero or more output elements using the provided stream-returning function.
     */
    default <T> LCollection<T> flatMap(SFunction<S, Stream<T>> fn, PType<T> pType) {
        return parallelDo(ctx -> fn.apply(ctx.element()).forEach(ctx::emit), pType);
    }

    /**
     * Map each element to zero or more output elements using the provided stream-returning function to yield an
     * {@link LTable}
     */
    default <K, V> LTable<K, V> flatMap(SFunction<S, Stream<Pair<K, V>>> fn, PTableType<K, V> pType) {
        return parallelDo(ctx -> fn.apply(ctx.element()).forEach(ctx::emit), pType);
    }

    /**
     * Combination of a filter and map operation by using a function with {@link Optional} return type.
     */
    default <T> LCollection<T> filterMap(SFunction<S, Optional<T>> fn, PType<T> pType) {
        return parallelDo(ctx -> fn.apply(ctx.element()).ifPresent(ctx::emit), pType);
    }

    /**
     * Combination of a filter and map operation by using a function with {@link Optional} return type.
     */
    default <K, V> LTable<K, V> filterMap(SFunction<S, Optional<Pair<K, V>>> fn, PTableType<K, V> pType) {
        return parallelDo(ctx -> fn.apply(ctx.element()).ifPresent(ctx::emit), pType);
    }

    /**
     * Filter the collection using the supplied predicate.
     */
    default LCollection<S> filter(SPredicate<S> predicate) {
        return parallelDo(ctx -> { if (predicate.test(ctx.element())) ctx.emit(ctx.element());}, pType());
    }

    /**
     * Union this LCollection with another LCollection of the same type
     */
    default LCollection<S> union(LCollection<S> other) {
        return factory().wrap(underlying().union(other.underlying()));
    }

    /**
     * Union this LCollection with a {@link PCollection} of the same type
     */
    default LCollection<S> union(PCollection<S> other) {
        return factory().wrap(underlying().union(other));
    }

    /**
     * Increment a counter for every element in the collection
     */
    default LCollection<S> increment(Enum<?> counter) {
        return parallelDo(ctx -> {
            ctx.increment(counter);
            ctx.emit(ctx.element());
        }, pType());
    }

    /**
     * Increment a counter for every element in the collection
     */
    default LCollection<S> increment(String counterGroup, String counterName) {
        return parallelDo(ctx -> {
            ctx.increment(counterGroup, counterName);
            ctx.emit(ctx.element());
        }, pType());
    }

    /**
     * Increment a counter for every element satisfying the conditional predicate supplied.
     */
    default LCollection<S> incrementIf(Enum<?> counter, SPredicate<S> condition) {
        return parallelDo(ctx -> {
            if (condition.test(ctx.element())) ctx.increment(counter);
            ctx.emit(ctx.element());
        }, pType());
    }

    /**
     * Increment a counter for every element satisfying the conditional predicate supplied.
     */
    default LCollection<S> incrementIf(String counterGroup, String counterName, SPredicate<S> condition) {
        return parallelDo(ctx -> {
            if (condition.test(ctx.element())) ctx.increment(counterGroup, counterName);
            ctx.emit(ctx.element());
        }, pType());
    }

    /**
     * Cache the underlying {@link PCollection}
     */
    default LCollection<S> cache() {
        underlying().cache();
        return this;
    }

    /**
     * Cache the underlying {@link PCollection}
     */
    default LCollection<S> cache(CachingOptions options) {
        underlying().cache(options);
        return this;
    }

    /**
     * Key this LCollection by a key extracted from the element to yield a {@link LTable} mapping the key to the whole
     * element.
     */
    default <K> LTable<K, S> by(SFunction<S, K> extractFn, PType<K> pType) {
        return parallelDo(
                ctx -> ctx.emit(Pair.of(extractFn.apply(ctx.element()), ctx.element())),
                ptf().tableOf(pType, pType()));
    }

    /**
     * Count distict values in this LCollection, yielding an {@link LTable} mapping each value to the number
     * of occurrences in the collection.
     */
    default LTable<S, Long> count() {
        return map(a -> Pair.of(a, 1L), ptf().tableOf(pType(), ptf().longs()))
                .groupByKey()
                .combineValues(Aggregators.SUM_LONGS());
    }

    /**
     * Obtain the contents of this LCollection as a {@link Stream} that can be processed locally. Note, this may trigger
     * your job to execute in a distributed environment if the pipeline has not yet been run.
     */
    default Stream<S> materialize() {
        return StreamSupport.stream(underlying().materialize().spliterator(), false);
    }

    /**
     * Get the {@link PTypeFamily} representing how elements of this collection may be serialized.
     */
    default PTypeFamily ptf() {
        return underlying().getPType().getFamily();
    }

    /**
     * Get the {@link PType} representing how elements of this collection may be serialized.
     */
    default PType<S> pType() { return underlying().getPType(); }

    /**
     * Write this collection to the specified {@link Target}
     */
    default LCollection<S> write(Target target) {
        underlying().write(target);
        return this;
    }

    /**
     * Write this collection to the specified {@link Target} with the given {@link org.apache.crunch.Target.WriteMode}
     */
    default LCollection<S> write(Target target, Target.WriteMode writeMode) {
        underlying().write(target, writeMode);
        return this;
    }

}
