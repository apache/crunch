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

package org.apache.crunch.impl.spark;


import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.apache.crunch.PCollection;
import org.apache.crunch.PObject;
import org.apache.crunch.Pipeline;
import org.apache.crunch.types.avro.Avros;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;

import javax.annotation.Nullable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class SmallCollectionLengthTest implements Serializable {


  @Test
  public void smallCollectionsShouldNotHaveNullLength() throws Exception {
    Pipeline p = new SparkPipeline("local", "foobar");
    final ImmutableList<String>
        allFruits =
        ImmutableList.of("apelsin", "banan", "citron", "daddel");
    final ArrayList<ImmutableList<String>> fruitLists = new ArrayList<>();
    for (int i = 0; i <= allFruits.size(); ++i) {
      fruitLists.add(ImmutableList.copyOf(allFruits.subList(0, i)));
    }

    final ArrayList<PObject<Long>> results = new ArrayList<>();
    for (ImmutableList<String> fruit : fruitLists) {
      final PCollection<String> collection = p.create(fruit, Avros.strings());
      results.add(collection.length());
    }

    p.run();

    final Iterable<Long>
        lengths =
        Iterables.transform(results, new Function<PObject<Long>, Long>() {
          @Nullable
          @Override
          public Long apply(@Nullable PObject<Long> input) {
            return input.getValue();
          }
        });

    for (Long length : lengths) {
      assertThat(length, not(nullValue()));
    }

    p.done();
  }

}
