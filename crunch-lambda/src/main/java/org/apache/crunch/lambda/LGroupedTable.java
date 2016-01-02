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

import org.apache.crunch.Aggregator;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.Pair;
import org.apache.crunch.lambda.fn.SBiConsumer;
import org.apache.crunch.lambda.fn.SBiFunction;
import org.apache.crunch.lambda.fn.SBinaryOperator;
import org.apache.crunch.lambda.fn.SFunction;
import org.apache.crunch.lambda.fn.SSupplier;
import org.apache.crunch.types.PType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
/**
 * Java 8 friendly version of the {@link PGroupedTable} interface, allowing distributed operations to be expressed in
 * terms of lambda expressions and method references, instead of creating a new class implementation for each operation.
 * @param <K> key type for this table
 * @param <V> value type for this table
 */
public interface LGroupedTable<K, V> extends LCollection<Pair<K, Iterable<V>>>  {
    /**
     * Get the underlying {@link PGroupedTable} for this LGroupedTable
     */
    PGroupedTable<K, V> underlying();

    /**
     * Combine the value part of the table using the provided Crunch {@link Aggregator}. This will be optimised into
     * both a combine and reduce in the MapReduce implementation, with similar optimisations available for other
     * implementations.
     */
    default LTable<K, V> combineValues(Aggregator<V> aggregator) {
        return factory().wrap(underlying().combineValues(aggregator));
    }

    /**
     * Combine the value part of the table using the given functions. The supplier is used to create a new aggregating
     * type, the combineFn adds a value into the aggregate, and the output function transforms the aggregate into
     * an iterable of the original value type. For example, summation can be expressed as follows:
     *
     * <pre>{@code myGroupedTable.combineValues(() -> 0, (sum, value) -> sum + value, Collections::singleton) }</pre>
     *
     * <p>This will be optimised into both a combine and reduce in the MapReduce implementation, with similar
     * optimizations *available for other implementations.</p>
     */
    default <A> LTable<K, V> combineValues(
            SSupplier<A> initialSupplier,
            SBiFunction<A, V, A> combineFn,
            SFunction<A, Iterable<V>> outputFn) {
        return combineValues(new LAggregator<>(initialSupplier, combineFn, outputFn));
    }

    /**
     * Map the values in this LGroupedTable using a custom function. This function operates over a stream which can
     * be consumed only once.
     *
     * <p>Note that in serialization systems which heavily reuse objects (such as Avro), you may
     * in fact get given the same object multiple times with different data as you consume the stream, meaning it may
     * be necessary to detach values.</p>
     */
    default <T> LTable<K, T> mapValues(SFunction<Stream<V>, T> fn, PType<T> pType) {
        return parallelDo(
                ctx -> ctx.emit(Pair.of(
                        ctx.element().first(),
                        fn.apply(StreamSupport.stream(ctx.element().second().spliterator(), false)))
                ), ptf().tableOf(keyType(), pType));
    }

    /**
     * Collect the values into an aggregate type. This differs from combineValues in that it outputs the aggregate type
     * rather than the value type, and is designed to happen in one step (rather than being optimised into multiple
     * levels). This makes it much more suitable for assembling collections than computing simple numeric aggregates.
     *
     * <p>The supplier provides an "empty" object, then the consumer is called with each value. For example, to collect
     * all values into a {@link Collection}, one can do this:</p>
     * <pre>{@code
     * lgt.collectValues(ArrayList::new, Collection::add, lgt.ptf().collections(lgt.valueType()))
     * }</pre>
     *
     * <p>This is in fact the default implementation for the collectAllValues() method.</p>
     *
     * <p>Note that in serialization systems which heavily reuse objects (such as Avro), you may
     * in fact get given the same object multiple times with different data as you consume the stream, meaning it may
     * be necessary to detach values.</p>
     */
    default <C> LTable<K, C> collectValues(SSupplier<C> emptySupplier, SBiConsumer<C, V> addFn, PType<C> pType) {
        return parallelDo(ctx -> {
            C coll = emptySupplier.get();
            ctx.element().second().forEach(v -> addFn.accept(coll, v));
            ctx.emit(Pair.of(ctx.element().first(), coll));
        }, ptf().tableOf(keyType(), pType));
    }

    /**
     * Collect all values for each key into a {@link Collection}
     */
    default LTable<K, Collection<V>> collectAllValues() {
        return collectValues(ArrayList::new, Collection::add, ptf().collections(valueType()));
    }

    /**
     * Collect all unique values for each key into a {@link Collection} (note that the value type must have a correctly-
     * defined equals() and hashcode().
     */
    default LTable<K, Collection<V>> collectUniqueValues() {
        return collectValues(HashSet::new, Collection::add, ptf().collections(valueType()));
    }

    /**
     * Reduce the values for each key using the an associative binary operator.
     * For example {@code reduceValues((a, b) -> a + b)} for summation, {@code reduceValues((a, b) -> a + ", " + b}
     * for comma-separated string concatenation and {@code reduceValues((a, b) -> a > b ? a : b} for maximum value.
     */
    default LTable<K, V> reduceValues(SBinaryOperator<V> operator) {
        return combineValues(() -> (V)null, (a, b) -> a == null ? b : operator.apply(a, b), Collections::singleton);
    }

    /**
     * Ungroup this LGroupedTable back into an {@link LTable}. This will still trigger a "reduce" operation, so is
     * usually only used in special cases like producing a globally-ordered list by feeding the everything through
     * a single reducers.
     */
    default LTable<K, V> ungroup() {
        return factory().wrap(underlying().ungroup());
    }

    /**
     * Get a {@link PType} which can be used to serialize the key part of this grouped table
     */
    default PType<K> keyType() {
        return underlying().getGroupedTableType().getTableType().getKeyType();
    }

    /**
     * Get a {@link PType} which can be used to serialize the value part of this grouped table
     */
    default PType<V> valueType() {
        return underlying().getGroupedTableType().getTableType().getValueType();
    }

}
