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

class PartialFunctionTest extends CrunchSuite {
  @Test def testPartialFunction {
    val pf = Mem.collectionOf(1, 2, 3).collect({ case i: Int if i != 3 => i + 1 })
    org.junit.Assert.assertEquals(List(2, 3), pf.materialize().toList)
  }

  @Test def testPartialFunctionPTable {
    val pf = Mem.tableOf("a" -> 1, "b" -> 2, "c" -> 3).collect({ case (k, v) if k != "c" => v + 1 })
    org.junit.Assert.assertEquals(List(2, 3), pf.materialize().toList)
  }
}
