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
package org.apache.crunch;

import com.google.common.base.Splitter;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.join.BloomFilterJoinStrategy;
import org.apache.crunch.lib.join.JoinType;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.junit.Rule;
import org.junit.Test;

import java.io.Serializable;
import java.util.Iterator;

import static org.apache.crunch.types.avro.Avros.strings;
import static org.apache.crunch.types.avro.Avros.tableOf;
import static org.junit.Assert.assertTrue;

public class MultiStagePlanningIT implements Serializable {
    @Rule
    public transient TemporaryPath tmpDir = TemporaryPaths.create();

    @Test
    public void testMultiStagePlanning() throws Exception {
        Pipeline pipeline = new MRPipeline(MRPipelineIT.class, tmpDir.getDefaultConfiguration());

        String customersFile = tmpDir.copyResourceFileName("customers.txt");
        String ordersFile = tmpDir.copyResourceFileName("orders.txt");
        String addressesFile = tmpDir.copyResourceFileName("addresses.txt");
        PTable<String, String> customersTable = pipeline.readTextFile(customersFile)
                .parallelDo("Split customers", new StringToPairMapFn(), tableOf(strings(), strings()));
        PTable<String, String> ordersTable = pipeline.readTextFile(ordersFile)
                .parallelDo("Split orders", new StringToPairMapFn(), tableOf(strings(), strings()));

        PTable<String, String> assignedOrders = new BloomFilterJoinStrategy<String, String, String>(5)
                .join(customersTable, ordersTable, JoinType.INNER_JOIN)
                .parallelDo(new MapFn<Pair<String, Pair<String, String>>, Pair<String, String>>() {
                    @Override
                    public Pair<String, String> map(Pair<String, Pair<String, String>> input) {
                        return Pair.of(input.first(), input.second().second());
                    }
                }, tableOf(strings(), strings()));

        PTable<String, String> addressesTable = pipeline.readTextFile(addressesFile)
                .parallelDo("Split addresses", new StringToPairMapFn(), tableOf(strings(), strings()))
                .filter(new IFilterFn<Pair<String, String>>() {
                    @Override
                    public boolean accept(Pair<String, String> input) {
                        // This is odd but it is the simpler way of simulating this would take longer than
                        // the other branch with the Bloom Filter ...
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        return true;
                    }
                });
        addressesTable.materialize();

        PTable<String, Pair<String, String>> orderAddresses = assignedOrders.join(addressesTable);
        orderAddresses.materialize();

        PipelineResult result = pipeline.run();
        assertTrue(result != null && result.succeeded());
    }

    private static class StringToPairMapFn extends MapFn<String, Pair<String, String>> {
        private transient Splitter splitter;

        @Override
        public void initialize() {
            super.initialize();
            splitter = Splitter.on('|');
        }

        @Override
        public Pair<String, String> map(String input) {
            Iterator<String> split = splitter.split(input).iterator();
            return Pair.of(split.next(), split.next());
        }
    }
}
