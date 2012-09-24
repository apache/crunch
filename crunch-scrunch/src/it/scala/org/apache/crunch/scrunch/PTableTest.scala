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

import org.apache.crunch.io.{From => from, To => to}
import org.apache.crunch.test.CrunchTestSupport

import _root_.org.junit.Assert._
import _root_.org.junit.Test
import org.scalatest.junit.JUnitSuite

/**
 * Tests functionality of Scala PTable.
 */
class PTableTest extends CrunchTestSupport with JUnitSuite {

  /**
   * Gets a PCollection containing the lines from the tens data set.
   *
   * @return The PCollection containing the test data set.
   */
  private def tensCollection: PCollection[Int] = {
    val pipeline = Pipeline.mapReduce[PTableTest](tempDir.getDefaultConfiguration)
    val input = tempDir.copyResourceFileName("tens.txt")
    pipeline.read(from.textFile(input)).map { line =>
      Integer.parseInt(line)
    }
  }

  /**
   * Tests retrieving the contents of a PTable as a Map.
   */
  @Test def testAsSeq {
    val tens = tensCollection
    val tensCounts = tens.count().asMap().value()
    assertEquals("Wrong map size for Map[Int, Long] obtained from count of tens collection.",
        1, tensCounts.size)
    assertTrue("Map of tens counts does not contain expected key.", tensCounts.contains(10))
    assertEquals("Wrong count of tens.", 100L, tensCounts(10))
  }
}

