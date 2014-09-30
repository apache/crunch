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

import org.apache.crunch.types.avro.AvroMode

import org.scalatest.junit.JUnitSuite
import org.junit.Test

/** Case classes for testing purposes */
case class One(a: Int, b: String, c: List[java.lang.Long], d: Array[Long])
case class Two(a: One, b: Set[Option[Boolean]], c: Map[String, Double], d: Map[Int, String])
case class Three(a: List[One], b: Array[Either[One, Two]])

class TupleNTest extends JUnitSuite{
  @Test def testTupleN {
    val pc = Mem.collectionOf((1, 2, "a", 3, "b"), (4, 5, "a", 6, "c"))
    val res = pc.map(x => (x._3, x._4)).groupByKey.combineValues(Aggregators.sum[Int]).materialize
    org.junit.Assert.assertEquals(List(("a", 9)), res.toList)
  }

  @Test def testJavaEnums {
    val pc = Mem.collectionOf((1, AvroMode.GENERIC), (2, AvroMode.SPECIFIC), (3, AvroMode.REFLECT))
    val res = pc.map(x => (x._2, x._1)).filter((k, v) => k == AvroMode.SPECIFIC).materialize
    org.junit.Assert.assertEquals(List((AvroMode.SPECIFIC, 2)), res.toList)
  }

  /**
   * Basically, we just want to validate that we can generate schemas for these classes successfully
   */
  val ones = Array(One(1, "a", List(17L, 29L), Array(12L, 13L)), One(2, "b", List(0L), Array(17L, 29L)))
  val twos = Array(Two(ones(0), Set(Some(true), None), Map("a" -> 1.2, "b" -> 2.9), Map(1 -> "a", 2 -> "b")))
  val threes = Array(Three(ones.toList, Array(Left(ones(0)), Right(twos(0)))))

  @Test def onesTest {
    val pc = Mem.collectionOf(ones : _*)
  }

  @Test def twosTest {
    val pc = Mem.collectionOf(twos : _*)
  }

  @Test def threesTest {
    val pc = Mem.collectionOf(threes : _*)
  }
}
