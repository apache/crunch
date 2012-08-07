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
package org.apache.crunch.scrunch

import org.scalatest.junit.JUnitSuite
import _root_.org.junit.Test

class CrossJoinTest extends JUnitSuite {

  @Test
  def testCrossCollection() {
    val testCases = List(Array(1,2,3,4,5), Array(6,7,8), Array.empty[Int])
    val testCasePairs = testCases flatMap {test1 => testCases map {test2 => (test1,test2)}}

    for ((test1, test2) <- testCasePairs) {
      val X = Mem.collectionOf(test1: _*)
      val Y = Mem.collectionOf(test2: _*)
      val cross = X.cross(Y)

      val crossSet = cross.materialize().toSet

      assert(crossSet.size == test1.size * test2.size)
      assert(test1.flatMap(t1 => test2.map(t2 => crossSet.contains((t1, t2)))).forall(_ == true))

    }
  }

  @Test
  def testCrossTable() {
    val testCases = List(Array((1,2),(3,4),(5,6)), Array((7,8),(9,10)), Array.empty[(Int,Int)])
    val testCasePairs = testCases flatMap {test1 => testCases map {test2 => (test1,test2)}}

    for ((test1, test2) <- testCasePairs) {
      val X = Mem.tableOf(test1)
      val Y = Mem.tableOf(test2)
      val cross = X.cross(Y)

      val crossSet = cross.materializeToMap().toSet
      val actualCross = test1.flatMap(t1 => test2.map(t2 => ((t1._1, t2._1), (t1._2, t2._2))))

      assert(crossSet.size == test1.size * test2.size)
      assert(actualCross.map(crossSet.contains(_)).forall(_ == true))
    }
  }

}
