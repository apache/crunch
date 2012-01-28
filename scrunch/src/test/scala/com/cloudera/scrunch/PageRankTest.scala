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
import _root_.org.junit.Assert._
import _root_.org.junit.Test

class PageRankTest extends JUnitSuite {
  val pipeline = new Pipeline[PageRankTest]

  def initialInput(fileName: String) = {
    pipeline.read(from.textFile(fileName))
      .map(line => { val urls = line.split("\\t"); (urls(0), urls(1)) })
      .groupByKey
      .map((url, links) => (url, (1f, 0f, links.toList.toIterable)))
  }

  def update(prev: PTable[String, (Float, Float, Iterable[String])], d: Float) = {
    val outbound = prev.flatMap((url, v) => {
      val (pr, oldpr, links) = v
      links.map(link => (link, pr / links.size))
    })
    prev.cogroup(outbound).map((url, v) => {
      val (p, o) = v
      val (pr, oldpr, links) = p.head
      (url, ((1 - d) + d * o.sum, pr, links))
    })
  }

  @Test def testPageRank {
    var prev = initialInput(FileHelper.createTempCopyOf("urls.txt"))
    var delta = 1.0f
    while (delta > 0.01f) {
      prev = update(prev, 0.5f)
      delta = prev.map((k, v) => math.abs(v._1 - v._2)).max.materialize.head
    }
    assertEquals(0.0048, delta, 0.001)
    pipeline.done
  }
}
