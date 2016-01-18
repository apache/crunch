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

import org.apache.crunch.GroupingOptions;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Target;
import org.apache.crunch.lambda.fn.SFunction;
import org.apache.crunch.lambda.fn.SPredicate;
import org.apache.crunch.lib.join.DefaultJoinStrategy;
import org.apache.crunch.lib.join.JoinStrategy;
import org.apache.crunch.lib.join.JoinType;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;

import java.util.Collection;

/**
 * Java 8 friendly version of the {@link PTable} interface, allowing distributed operations to be expressed in
 * terms of lambda expressions and method references, instead of creating a new class implementation for each operation.
 * @param <K> key type for this table
 * @param <V> value type for this table
 */
public interface LTable<K, V> extends LCollection<Pair<K, V>> {
    /**
     * Get the underlying {@link PTable} for this LCollection
     */
    PTable<K, V> underlying();

    /**
     * Group this table by key to yield a {@link LGroupedTable}
     */
    default LGroupedTable<K, V> groupByKey() {
        return factory().wrap(underlying().groupByKey());
    }

    /**
     * Group this table by key to yield a {@link LGroupedTable}
     */
    default LGroupedTable<K, V> groupByKey(int numReducers) {
        return factory().wrap(underlying().groupByKey(numReducers));
    }

    /**
     * Group this table by key to yield a {@link LGroupedTable}
     */
    default LGroupedTable<K, V> groupByKey(GroupingOptions opts) {
        return factory().wrap(underlying().groupByKey(opts));
    }

    /**
     * Get an {@link LCollection} containing just the keys from this table
     */
    default LCollection<K> keys() {
        return factory().wrap(underlying().keys());
    }

    /**
     * Get an {@link LCollection} containing just the values from this table
     */
    default LCollection<V> values() {
        return factory().wrap(underlying().values());
    }

    /**
     * Transform the keys of this table using the given function
     */
    default <T> LTable<T, V> mapKeys(SFunction<K, T> fn, PType<T> pType) {
        return parallelDo(
                ctx -> ctx.emit(Pair.of(fn.apply(ctx.element().first()), ctx.element().second())),
                ptf().tableOf(pType, valueType()));
    }

    /**
     * Transform the values of this table using the given function
     */
    default <T> LTable<K, T> mapValues(SFunction<V, T> fn, PType<T> pType) {
        return parallelDo(
                ctx -> ctx.emit(Pair.of(ctx.element().first(), fn.apply(ctx.element().second()))),
                ptf().tableOf(keyType(), pType));
    }

    /**
     * Filter the rows of the table using the supplied predicate.
     */
    default LTable<K, V> filter(SPredicate<Pair<K, V>> predicate) {
        return parallelDo(ctx -> { if (predicate.test(ctx.element())) ctx.emit(ctx.element());}, pType());
    }

    /**
     * Filter the rows of the table using the supplied predicate applied to the key part of each record.
     */
    default LTable<K, V> filterByKey(SPredicate<K> predicate) {
        return parallelDo(ctx -> { if (predicate.test(ctx.element().first())) ctx.emit(ctx.element());}, pType());
    }

    /**
     * Filter the rows of the table using the supplied predicate applied to the value part of each record.
     */
    default LTable<K, V> filterByValue(SPredicate<V> predicate) {
        return parallelDo(ctx -> { if (predicate.test(ctx.element().second())) ctx.emit(ctx.element());}, pType());
    }

    /**
     * Join this table to another {@link LTable} which has the same key type using the provided {@link JoinType} and
     * {@link JoinStrategy}
     */
    default <U> LTable<K, Pair<V, U>> join(LTable<K, U> other, JoinType joinType, JoinStrategy<K, V, U> joinStrategy) {
        return factory().wrap(joinStrategy.join(underlying(), other.underlying(), joinType));
    }

    /**
     * Join this table to another {@link LTable} which has the same key type using the provide {@link JoinType} and
     * the {@link DefaultJoinStrategy} (reduce-side join).
     */
    default <U> LTable<K, Pair<V, U>> join(LTable<K, U> other, JoinType joinType) {
        return join(other, joinType, new DefaultJoinStrategy<>());
    }

    /**
     * Inner join this table to another {@link LTable} which has the same key type using a reduce-side join
     */
    default <U> LTable<K, Pair<V, U>> join(LTable<K, U> other) {
        return join(other, JoinType.INNER_JOIN);
    }

    /**
     * Cogroup this table with another {@link LTable} with the same key type, collecting the set of values from
     * each side.
     */
    default <U> LTable<K, Pair<Collection<V>, Collection<U>>> cogroup(LTable<K, U> other) {
        return factory().wrap(underlying().cogroup(other.underlying()));
    }

    /**
     * Get the underlying {@link PTableType} used to serialize key/value pairs in this table
     */
    default PTableType<K, V> pType() { return underlying().getPTableType(); }

    /**
     * Get a {@link PType} which can be used to serialize the key part of this table
     */
    default PType<K> keyType() {
        return underlying().getKeyType();
    }

    /**
     * Get a {@link PType} which can be used to serialize the value part of this table
     */
    default PType<V> valueType() {
        return underlying().getValueType();
    }

    /**
     * Write this table to the {@link Target} supplied.
     */
    default LTable<K, V> write(Target target) {
        underlying().write(target);
        return this;
    }

    /**
     * Write this table to the {@link Target} supplied.
     */
    default LTable<K, V> write(Target target, Target.WriteMode writeMode) {
        underlying().write(target, writeMode);
        return this;
    }

    /** {@inheritDoc} */
    default LTable<K, V> increment(Enum<?> counter) {
        return parallelDo(ctx -> ctx.increment(counter), pType());
    }

    /** {@inheritDoc} */
    default LTable<K, V> increment(String counterGroup, String counterName) {
        return parallelDo(ctx -> ctx.increment(counterGroup, counterName), pType());
    }

    /** {@inheritDoc} */
    default LTable<K, V> incrementIf(Enum<?> counter, SPredicate<Pair<K, V>> condition) {
        return parallelDo(ctx -> {
            if (condition.test(ctx.element())) ctx.increment(counter);
        }, pType());
    }

    /** {@inheritDoc} */
    default LTable<K, V> incrementIf(String counterGroup, String counterName, SPredicate<Pair<K, V>> condition) {
        return parallelDo(ctx -> {
            if (condition.test(ctx.element())) ctx.increment(counterGroup, counterName);
        }, pType());
    }
}
