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

import org.scalatest.junit.JUnitSuite
import _root_.org.junit.Test

/**
 * Tests computing the number of elements in a PCollection from the Scala api.
 */
class LengthTest extends CrunchTestSupport with JUnitSuite {

  @Test def testLength {
    val linesInShakespeare: Long = 3667
    val pipeline = Pipeline.mapReduce[LengthTest](tempDir.getDefaultConfiguration)
    val input = tempDir.copyResourceFileName("shakes.txt")

    val len = pipeline.read(from.textFile(input)).length()
    assert(linesInShakespeare == len.value())
  }
}
