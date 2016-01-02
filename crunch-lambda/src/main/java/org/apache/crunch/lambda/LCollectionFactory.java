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

import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;

/**
 * Factory for creating {@link LCollection}, {@link LTable} and {@link LGroupedTable} objects from their corresponding
 * {@link PCollection}, {@link PTable} and {@link PGroupedTable} types. You probably don't want to use or implement this
 * interface directly. You should start with the {@link Lambda} class instead.
 */
public interface LCollectionFactory {
    /**
     * Wrap a PCollection into an LCollection
     */
    <S> LCollection<S> wrap(PCollection<S> collection);

    /**
     * Wrap a PTable into an LTable
     */
    <K, V> LTable<K, V> wrap(PTable<K, V> collection);

    /**
     * Wrap a PGroupedTable into an LGroupedTable
     */
    <K, V> LGroupedTable<K, V> wrap(PGroupedTable<K, V> collection);
}
