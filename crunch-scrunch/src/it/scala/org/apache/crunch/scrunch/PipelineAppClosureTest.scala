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

import org.apache.crunch.test.CrunchTestSupport

import org.scalatest.junit.JUnitSuite
import _root_.org.junit.Assert._
import _root_.org.junit.Test

/**
 * Test that verifies that a Scala PipelineApp can properly send some side data as part of a
 * function closure.
 */
class PipelineClosureAppTest extends CrunchTestSupport with JUnitSuite {

  /**
   * A simple pipeline application that divides each element of a PCollection of numbers by
   * 10. The PCollection of numbers used as input is just the number 10 repeated 100 times.
   * Thus the resulting PCollection should be the number 1 repeated 100 times.
   */
  object Divider extends PipelineApp {

    /**
     * Runs the Pipeline for this test and verifies it has the desired effect of transforming a
     * PCollection of 10s into a PCollection of 1s.
     */
    override def run(args: Array[String]) {
      val divisor = 10
      val tens = read(from.textFile(args(0)))
      val ones = tens.map { n => Integer.valueOf(n) / divisor }
      val countOfOnes = ones.count().materializeToMap()
      assertEquals(100, countOfOnes(1))
    }
  }

  @Test def run {
    val args = new Array[String](1)
    args(0) = tempDir.copyResourceFileName("tens.txt")
    tempDir.overridePathProperties(Divider.configuration)
    Divider.main(args)
  }
}
