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

import Avros._

import com.cloudera.crunch.{DoFn, Emitter, Pair => P}
import com.cloudera.crunch.io.{From => from}
import com.cloudera.crunch.test.FileHelper

import scala.collection.mutable.HashMap

import org.scalatest.junit.JUnitSuite
import _root_.org.junit.Assert._
import _root_.org.junit.Test

case class PageRankData(pr: Float, oldpr: Float, urls: Array[String]) {
  def this() = this(0f, 0f, null)

  def scaledPageRank = pr / urls.length

  def next(newPageRank: Float) = new PageRankData(newPageRank, pr, urls)

  def delta = math.abs(pr - oldpr)
}

class CachingPageRankClassFn extends DoFn[P[String, PageRankData], P[String, Float]] {
  val cache = new HashMap[String, Float] {
    override def default(key: String) = 0f
  }

  override def process(input: P[String, PageRankData], emitFn: Emitter[P[String, Float]]) {
    val prd = input.second()
    if (prd.urls.length > 0) {
      val newpr = prd.pr / prd.urls.length
      prd.urls.foreach(url => cache.put(url, cache(url) + newpr))
      if (cache.size > 5000) {
        cleanup(emitFn)
      }
    }
  }

  override def cleanup(emitFn: Emitter[P[String, Float]]) {
    cache.foreach(kv => emitFn.emit(P.of(kv._1, kv._2)))
    cache.clear
  }
}

class PageRankClassTest extends JUnitSuite {
  val pipeline = new Pipeline[PageRankTest]

  def initialInput(fileName: String) = {
    pipeline.read(from.textFile(fileName))
      .map(line => { val urls = line.split("\\t"); (urls(0), urls(1)) })
      .groupByKey
      .map((url, links) => (url, PageRankData(1f, 0f, links.filter(x => x != null).toArray)))
  }

  def update(prev: PTable[String, PageRankData], d: Float) = {
    val outbound = prev.flatMap((url, prd) => {
      prd.urls.map(link => (link, prd.scaledPageRank))
    })
    cg(prev, outbound, d)
  }

  def cg(prev: PTable[String, PageRankData],
         out: PTable[String, Float], d: Float) = {
    prev.cogroup(out).map((url, v) => {
      val (p, o) = v
      val prd = p.head
      (url, prd.next((1 - d) + d * o.sum))
    })
  }

  def fastUpdate(prev: PTable[String, PageRankData], d: Float) = {
    val outbound = prev.parallelDo(new CachingPageRankClassFn(), tableOf(strings, floats))
    cg(prev, outbound, d)
  }

  @Test def testPageRank {
    pipeline.getConfiguration.set("crunch.debug", "true")
    var prev = initialInput(FileHelper.createTempCopyOf("urls.txt"))
    var delta = 1.0f
    while (delta > 0.01f) {
      prev = update(prev, 0.5f)
      delta = prev.values.map(_.delta).max.materialize.head
    }
    assertEquals(0.0048, delta, 0.001)
    pipeline.done
  }

  def testFastPageRank {
    pipeline.getConfiguration.set("crunch.debug", "true")
    var prev = initialInput(FileHelper.createTempCopyOf("urls.txt"))
    var delta = 1.0f
    while (delta > 0.01f) {
      prev = fastUpdate(prev, 0.5f)
      delta = prev.values.map(_.delta).max.materialize.head
    }
    assertEquals(0.0048, delta, 0.001)
    pipeline.done
  }
}
