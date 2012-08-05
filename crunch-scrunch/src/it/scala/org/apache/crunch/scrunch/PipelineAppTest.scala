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
import _root_.org.junit.Test

object WordCount extends PipelineApp {

  def wordSplit(line: String) = line.split("\\W+").filter(!_.isEmpty())

  def countWords(filename: String) = {
    val lines = read(from.textFile(filename))
    val words = lines.flatMap(wordSplit)
    words.count
  }

  val w1 = countWords(args(0))
  val w2 = countWords(args(1))
  cogroup(w1, w2).write(to.textFile(args(2)))
}

class PipelineAppTest extends CrunchTestSupport with JUnitSuite {
  @Test def run {
    val args = new Array[String](3)
    args(0) = tempDir.copyResourceFileName("shakes.txt")
    args(1) = tempDir.copyResourceFileName("maugham.txt")
    args(2) = tempDir.getFileName("output")
    tempDir.overridePathProperties(WordCount.configuration)
    WordCount.main(args)
  }
}
