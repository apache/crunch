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

import org.scalatest.junit.JUnitSuite
import _root_.org.junit.{Assert, Test}

class AggregatorsTest extends JUnitSuite {

  @Test def testSum {
    val pc = Mem.collectionOf((1, 0.1), (2, 1.2), (3, 2.2), (4, 2.0), (5, 0.0))
    val sum = pc.aggregate(Aggregators.sum).materialize().toList
    Assert.assertEquals(1, sum.size)
    Assert.assertEquals(15, sum(0)._1)
    Assert.assertEquals(5.5, sum(0)._2, 0.001)
  }

  @Test def testMin {
    val pc = Mem.collectionOf(1, 2, 3, 4, 5)
    val min = pc.aggregate(Aggregators.min).materialize().toList
    Assert.assertEquals(1, min.size)
    Assert.assertEquals(1, min(0))
  }

  @Test def testMax {
    val pc = Mem.collectionOf(1, 2, 3, 4, 5)
    val max = pc.aggregate(Aggregators.max).materialize().toList
    Assert.assertEquals(1, max.size)
    Assert.assertEquals(5, max(0))
  }
}
