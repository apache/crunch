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

class LCollectionFactoryImpl implements LCollectionFactory {

    @Override
    public <S> LCollection<S> wrap(final PCollection<S> collection) {
        return new LCollection<S>() {
            @Override
            public PCollection<S> underlying() {
                return collection;
            }

            @Override
            public LCollectionFactory factory() {
                return LCollectionFactoryImpl.this;
            }
        };
    }

    @Override
    public <K, V> LTable<K, V> wrap(final PTable<K, V> collection) {
        return new LTable<K, V>() {
            @Override
            public PTable<K, V> underlying() {
                return collection;
            }

            @Override
            public LCollectionFactory factory() {
                return LCollectionFactoryImpl.this;
            }
        };
    }

    @Override
    public <K, V> LGroupedTable<K, V> wrap(final PGroupedTable<K, V> collection) {
        return new LGroupedTable<K, V>() {
            @Override
            public PGroupedTable<K, V> underlying() {
                return collection;
            }

            @Override
            public LCollectionFactory factory() {
                return LCollectionFactoryImpl.this;
            }
        };
    }
}
