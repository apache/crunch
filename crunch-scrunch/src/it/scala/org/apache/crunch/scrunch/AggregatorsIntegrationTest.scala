/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.crunch.scrunch

import org.junit.Test

class AggregatorsIntegrationTest extends CrunchSuite {
  @Test def productAggregators {
    val pipeline = Pipeline.mapReduce[WordCountTest](tempDir.getDefaultConfiguration)
    val input = tempDir.copyResourceFileName("shakes.txt")

    val fcc = pipeline.read(From.textFile(input))
      .flatMap(_.toLowerCase.split("\\s+"))
      .filter(!_.isEmpty())
      .map(word => (word.slice(0, 1), (1L, word.length)))
      .groupByKey
      .combineValues(Aggregators.product[(Long, Int)](Aggregators.sum[Long], Aggregators.max[Int]))
      .materialize
    assert(fcc.exists(_ == ("w", (1302, 12))))

    pipeline.done
  }
}
