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
import org.junit.Test

class TupleNTest extends JUnitSuite{
  @Test def testTupleN {
    val pc = Mem.collectionOf((1, 2, "a", 3, "b"), (4, 5, "a", 6, "c"))
    val res = pc.map(x => (x._3, x._4)).groupByKey.combineValues(Aggregators.sum[Int]).materialize
    org.junit.Assert.assertEquals(List(("a", 9)), res.toList)
  }
}
