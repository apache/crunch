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
package org.apache.scrunch

import _root_.org.scalatest.junit.JUnitSuite
import _root_.org.junit.Test

class MapKeysFnTest extends JUnitSuite {

  @Test
  def testMapKeys() {
    val orig = Mem.tableOf(1 -> "a", 2 -> "b", 3 -> "c")
    val inc = orig.mapKeys(_ + 1)

    assert(
      inc.keys.materialize
        .zip(orig.keys.materialize)
        .forall(x => x._1 == x._2 + 1))
  }

}