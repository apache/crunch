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
import com.google.common.collect.ImmutableSet;
import org.apache.crunch.Pair;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mem.MemPipeline;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static org.apache.crunch.lambda.TestCommon.assertCollectionOf;
import static org.apache.crunch.types.avro.Avros.*;


public class LGroupedTableTest {

    LGroupedTable<String, Integer> lgt = Lambda.wrap(MemPipeline.typedTableOf(tableOf(strings(), ints()),
            "a", 2,
            "a", 3,
            "b", 5,
            "c", 7,
            "c", 11,
            "c", 13,
            "c", 13))
            .groupByKey();

    @Test
    public void testCombineValues() throws Exception {
        assertCollectionOf(lgt.combineValues(Aggregators.MAX_INTS()),
                Pair.of("a", 3),
                Pair.of("b", 5),
                Pair.of("c", 13));
    }

    @Test
    public void testCombineValues1() throws Exception {
        assertCollectionOf(lgt.combineValues(() -> Integer.MIN_VALUE, Integer::max, Collections::singleton),
                Pair.of("a", 3),
                Pair.of("b", 5),
                Pair.of("c", 13));
    }

    @Test
    public void testMapValues() throws Exception {
        assertCollectionOf(lgt.mapValues(vs -> vs.map(i -> i.toString()).reduce((a, b) -> a + "," + b).get(), strings()),
                Pair.of("a", "2,3"),
                Pair.of("b", "5"),
                Pair.of("c", "7,11,13,13"));
    }

    @Test
    public void testCollectValues() throws Exception {
        assertCollectionOf(lgt.collectValues(ArrayList::new, Collection::add, collections(ints())),
                Pair.of("a", ImmutableList.of(2,3)),
                Pair.of("b", ImmutableList.of(5)),
                Pair.of("c", ImmutableList.of(7, 11, 13, 13)));
    }

    @Test
    public void testCollectAllValues() throws Exception {
        assertCollectionOf(lgt.collectAllValues(),
                Pair.of("a", ImmutableList.of(2,3)),
                Pair.of("b", ImmutableList.of(5)),
                Pair.of("c", ImmutableList.of(7, 11, 13, 13)));
    }

    @Test
    public void testCollectUniqueValues() throws Exception {
        assertCollectionOf(lgt.collectUniqueValues(),
                Pair.of("a", ImmutableSet.of(2, 3)),
                Pair.of("b", ImmutableSet.of(5)),
                Pair.of("c", ImmutableSet.of(7, 11, 13)));
    }

    @Test
    public void testReduceValues() throws Exception {
        assertCollectionOf(lgt.reduceValues((a, b) -> a * b),
                Pair.of("a", 6),
                Pair.of("b", 5),
                Pair.of("c", 7 * 11 * 13 * 13)
                );
    }
}