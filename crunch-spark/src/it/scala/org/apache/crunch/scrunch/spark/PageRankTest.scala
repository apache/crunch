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
package org.apache.crunch.scrunch.spark

import org.apache.crunch.scrunch.{PTable, Avros}
import org.apache.crunch.io.{From => from}

import _root_.org.junit.Assert._
import _root_.org.junit.Test

class PageRankTest extends CrunchSparkSuite {
  lazy val pipeline = SparkPipeline[PageRankTest](tempDir.getDefaultConfiguration)

  def initialInput(fileName: String) = {
    pipeline.read(from.textFile(fileName))
      .withPType(Avros.strings)
      .mapWithContext((line, ctxt) => {
        ctxt.getConfiguration; val urls = line.split("\\t"); (urls(0), urls(1))
      })
      .groupByKey
      .map((url, links) => (url, (1.0, 0.0, links.toList)))
  }

  def update(prev: PTable[String, (Double, Double, List[String])], d: Double) = {
    val outbound = prev.flatMap((url, v) => {
      val (pr, oldpr, links) = v
      links.map(link => (link, pr / links.size))
    })
    cg(prev, outbound, d)
  }

  def cg(prev: PTable[String, (Double, Double, List[String])],
         out: PTable[String, Double], d: Double) = {
    prev.cogroup(out).map((url, v) => {
      val (p, o) = v
      val (pr, oldpr, links) = p.head
      (url, ((1 - d) + d * o.sum, pr, links))
    })
  }

  @Test def testPageRank {
    var prev = initialInput(tempDir.copyResourceFileName("urls.txt"))
    var delta = 1.0
    while (delta > 0.01) {
      prev = update(prev, 0.5)
      delta = prev.map((k, v) => math.abs(v._1 - v._2)).max.value()
    }
    assertEquals(0.0048, delta, 0.001)
    pipeline.done
  }
}
