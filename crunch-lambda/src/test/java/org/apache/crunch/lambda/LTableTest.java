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

import com.google.common.collect.ImmutableList;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.junit.Test;

import static org.apache.crunch.lambda.TestCommon.assertCollectionOf;
import static org.apache.crunch.types.avro.Avros.*;


public class LTableTest {

    private LTable<String, Integer> lt1 = Lambda.wrap(MemPipeline.typedTableOf(tableOf(strings(), ints()),
            "a", 2,
            "a", 3,
            "b", 5,
            "c", 7,
            "c", 11,
            "c", 13,
            "c", 13));

    private LTable<String, Long> lt2 = Lambda.wrap(MemPipeline.typedTableOf(tableOf(strings(), longs()),
            "a", 101L,
            "b", 102L,
            "c", 103L
            ));

    @Test
    public void testKeys() throws Exception {
        assertCollectionOf(lt1.keys(), "a", "a", "b", "c", "c", "c", "c");
    }

    @Test
    public void testValues() throws Exception {
        assertCollectionOf(lt2.values(), 101L, 102L, 103L);
    }

    @Test
    public void testMapKeys() throws Exception {
        assertCollectionOf(lt2.mapKeys(String::toUpperCase, strings()),
                Pair.of("A", 101L),
                Pair.of("B", 102L),
                Pair.of("C", 103L)
                );
    }

    @Test
    public void testMapValues() throws Exception {
        assertCollectionOf(lt2.mapValues(v -> v * 2, longs()),
                Pair.of("a", 202L),
                Pair.of("b", 204L),
                Pair.of("c", 206L)
        );
    }

    @Test
    public void testJoin() throws Exception {
        assertCollectionOf(lt1.join(lt2).values(),
                Pair.of(2, 101L),
                Pair.of(3, 101L),
                Pair.of(5, 102L),
                Pair.of(7, 103L),
                Pair.of(11, 103L),
                Pair.of(13, 103L),
                Pair.of(13, 103L));
    }

    @Test
    public void testCogroup() throws Exception {
        assertCollectionOf(lt1.cogroup(lt2).values(),
                Pair.of(ImmutableList.of(2, 3), ImmutableList.of(101L)),
                Pair.of(ImmutableList.of(5), ImmutableList.of(102L)),
                Pair.of(ImmutableList.of(7, 11, 13, 13), ImmutableList.of(103L))
                );
    }
}