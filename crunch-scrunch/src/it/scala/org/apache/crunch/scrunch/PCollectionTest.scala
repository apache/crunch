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

import org.junit.Assert._
import org.junit.Test
import org.scalatest.junit.JUnitSuite

/**
 * Tests functionality of Scala PCollection.
 */
class PCollectionTest extends CrunchTestSupport with JUnitSuite {

  // Number of lines in the Shakespeare data set.
  val linesInShakespeare: Int = 3667

  // The first line in the Shakespeare data set.
  val firstLineInShakespeare: String =
      "***The Project Gutenberg's Etext of Shakespeare's First Folio***"

  // The last line in the Shakespeare data set.
  val lastLineInShakespeare: String =
      "FINIS. THE TRAGEDIE OF MACBETH."

  /**
   * Gets a PCollection containing the lines from the Shakespeare input text.
   *
   * @return The PCollection containing the test data set.
   */
  private def shakespeareCollection: PCollection[String] = {
    val pipeline = Pipeline.mapReduce[PCollectionTest](tempDir.getDefaultConfiguration)
    val input = tempDir.copyResourceFileName("shakes.txt")
    pipeline.read(from.textFile(input))
  }

  /**
   * Tests computing the number of elements in a PCollection via PCollection#length.
   */
  @Test def testLength {
    val len = shakespeareCollection.length().value()
    assertEquals("Wrong number of lines in Shakespeare.", linesInShakespeare, len)
  }

  /**
   * Tests retrieving the contents of a PCollection as a Seq.
   */
  @Test def testAsSeq {
    val lines = shakespeareCollection.asSeq().value()
    assertEquals("Wrong number of lines in Shakespeare.", linesInShakespeare, lines.length)
    assertEquals("Wrong first line in Shakespeare.", firstLineInShakespeare, lines(0))
    assertEquals("Wrong last line in Shakespeare.", lastLineInShakespeare,
        lines(linesInShakespeare - 1))
  }

  /**
   * Tests sampling elements from a PCollection using some acceptance probability.
   */
  @Test def testSampling {
    // Get the collection and sample ten percent.
    val shakespeare = shakespeareCollection
    val sampledCollection = shakespeare.sample(0.10)
    val length = sampledCollection.length().value()
    // The number of lines in the sampled collection should be about ten percent of the lines in
    // the original collection. We use a tolerance of +- 50.
    val lower = linesInShakespeare * 0.10 - 50
    val upper = linesInShakespeare * 0.10 + 50
    assertTrue("Sampled collection contains too few elements.", lower <= length)
    assertTrue("Sampled collection contains too many elements.", length <= upper)
  }

  /**
   * Tests sampling elements from a PCollection using some acceptance probability and a seed.
   */
  @Test def testSamplingWithSeed {
    // Get the collection and sample ten percent.
    val shakespeare = shakespeareCollection
    // With a seed of 1L, 380 elements should be sampled.
    val sampledCollection = shakespeare.sample(0.10, 1L)
    val length = sampledCollection.length().value()
    assertEquals("Incorrect number of elements sampled with seed 1L.", 380L, length)
  }
}
