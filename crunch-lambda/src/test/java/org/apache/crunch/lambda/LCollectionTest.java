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

import com.google.common.collect.ImmutableMap;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.avro.Avros;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static org.apache.crunch.lambda.TestCommon.*;
import static org.apache.crunch.lambda.TypedRecord.rec;
import static org.apache.crunch.types.avro.Avros.*;
import static org.junit.Assert.*;

public class LCollectionTest {

    private LCollection<TypedRecord> lc() {
        return Lambda.wrap(MemPipeline.typedCollectionOf(Avros.reflects(TypedRecord.class),
                rec(14, "Alice", 101L),
                rec(25, "Bo B", 102L),
                rec(21, "Char Lotte", 103L),
                rec(28, "David", 104L),
                rec(31, "Erik", 105L)));
    }

    @Test
    public void testParallelDo() throws Exception {
        LCollection<String> result = lc().parallelDo(ctx -> { if (ctx.element().key > 26) ctx.emit(ctx.element().name); }, strings());
        assertCollectionOf(result, "David", "Erik");
    }

    @Test
    public void testParallelDoPair() throws Exception {
        LTable<Integer, String> result = lc().parallelDo(ctx -> {
            if (ctx.element().key > 26) ctx.emit(Pair.of(ctx.element().key, ctx.element().name)); }, tableOf(ints(), strings()));
        assertCollectionOf(result, Pair.of(28, "David"), Pair.of(31, "Erik"));
    }


    @Test
    public void testMap() throws Exception {
        assertCollectionOf(lc().map(r -> r.key, ints()), 14, 25, 21, 28, 31);
    }

    @Test
    public void testMapPair() throws Exception {
        assertCollectionOf(lc().map(r -> Pair.of(r.key, r.value), tableOf(ints(), longs())),
                Pair.of(14, 101L),
                Pair.of(25, 102L),
                Pair.of(21, 103L),
                Pair.of(28, 104L),
                Pair.of(31, 105L));
    }

    @Test
    public void testFlatMap() throws Exception {
        assertCollectionOf(
                lc().flatMap(s -> Arrays.stream(s.name.split(" ")), strings()),
                "Alice", "Bo", "B", "Char", "Lotte", "David", "Erik");
    }


    @Test
    public void testFilterMap() throws Exception {
        Map<String, String> lookupMap = ImmutableMap.of("Erik", "BOOM", "Alice", "POW");
        assertCollectionOf(
                lc().filterMap(r -> lookupMap.containsKey(r.name) ? Optional.of(lookupMap.get(r.name)) : Optional.empty(), strings()),
                "BOOM", "POW"
        );
    }

    @Test
    public void testFilter() throws Exception {
        assertCollectionOf(lc().filter(r -> r.key == 21), rec(21, "Char Lotte", 103L));
    }


    @Test
    public void testIncrement() throws Exception {
        lc().increment("hello", "world");
        long value = MemPipeline.getCounters().findCounter("hello", "world").getValue();
        assertEquals(5L, value);
    }

    @Test
    public void testIncrementIf() throws Exception {
        lc().incrementIf("hello", "conditional_world", r -> r.key < 25);
        long value = MemPipeline.getCounters().findCounter("hello", "conditional_world").getValue();
        assertEquals(2L, value);
    }

    @Test
    public void testBy() throws Exception {
        assertCollectionOf(
                lc().filter(r -> r.key == 21).by(r -> r.key, ints()),
                Pair.of(21, rec(21, "Char Lotte", 103L)));
    }

    @Test
    public void testCount() throws Exception {
        assertCollectionOf(
                Lambda.wrap(MemPipeline.typedCollectionOf(strings(), "a", "a", "a", "b", "b")).count(),
                Pair.of("a", 3L),
                Pair.of("b", 2L)
        );
    }

}