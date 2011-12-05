/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.scrunch

import com.cloudera.crunch.io.{From => from}
import com.cloudera.crunch.test.FileHelper

import org.scalatest.junit.JUnitSuite
import _root_.org.junit.Test

class CogroupTest extends JUnitSuite {
  val pipeline = new Pipeline[CogroupTest]

  def wordCount(fileName: String) = {
    pipeline.read(from.textFile(fileName))
        .flatMap(_.toLowerCase.split("\\W+")).count
  }

  @Test def cogroup {
    val shakespeare = FileHelper.createTempCopyOf("shakes.txt")
    val maugham = FileHelper.createTempCopyOf("maugham.txt")
    val diffs = wordCount(shakespeare).cogroup(wordCount(maugham))
        .map((k, v) => (k, (v._1.sum - v._2.sum))).materialize
    assert(diffs.exists(_ == ("the", -11390)))
    pipeline.done
  }
}
