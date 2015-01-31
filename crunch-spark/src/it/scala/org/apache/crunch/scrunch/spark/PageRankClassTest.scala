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

import org.apache.crunch.scrunch._
import org.apache.crunch.scrunch.Avros._

import org.apache.crunch.{DoFn, Emitter, Pair => P}
import org.apache.crunch.io.{From => from}

import scala.collection.mutable.HashMap

import _root_.org.junit.Assert._
import _root_.org.junit.Test

class PageRankData(val page_rank: Float, oldpr: Float, val urls: Array[String], bytes: Array[Byte]) {

  // Required no-arg constructor for Avro reflection
  def this() = this(0.0f, 0.0f, null, null)

  def scaledPageRank = page_rank / urls.length

  def next(newPageRank: Float) = new PageRankData(newPageRank, page_rank, urls, bytes)

  def delta = math.abs(page_rank - oldpr)
}

class CachingPageRankClassFn extends DoFn[P[String, PageRankData], P[String, Float]] {
  val cache = new HashMap[String, Float] {
    override def default(key: String) = 0f
  }

  override def process(input: P[String, PageRankData], emitFn: Emitter[P[String, Float]]) {
    val prd = input.second()
    if (prd.urls.length > 0) {
      val newpr = prd.page_rank / prd.urls.length
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

class PageRankClassTest extends CrunchSparkSuite {

  lazy val pipeline = SparkPipeline[PageRankClassTest](tempDir.getDefaultConfiguration)

  def initialInput(fileName: String) = {
    pipeline.read(from.textFile(fileName, Avros.strings))
      .map(line => { val urls = line.split("\\t"); (urls(0), urls(1)) })
      .groupByKey
      .map((url, links) => (url, new PageRankData(1f, 0f, links.filter(x => x != null).toArray, Array[Byte](0))))
  }

  def update(prev: PTable[String, PageRankData], d: Array[Float]) = {
    val outbound = prev.flatMap((url, prd) => {
      prd.urls.map(link => (link, prd.scaledPageRank))
    })
    cg(prev, outbound, d)
  }

  def cg(prev: PTable[String, PageRankData],
         out: PTable[String, Float], d: Array[Float]) = {
    prev.cogroup(out).map((url, v) => {
      val (p, o) = v
      val prd = p.head
      (url, prd.next((1 - d(0)) + d(0) * o.sum))
    })
  }

  def fastUpdate(prev: PTable[String, PageRankData], d: Array[Float]) = {
    val outbound = prev.parallelDo(new CachingPageRankClassFn(), tableOf(strings, floats))
    cg(prev, outbound, d)
  }

  @Test def testPageRank {
    var prev = initialInput(tempDir.copyResourceFileName("urls.txt"))
    var delta = 1.0f
    while (delta > 0.01f) {
      prev = update(prev, Array(0.5f))
      delta = prev.values.map(_.delta).max.value()
    }
    assertEquals(0.0048, delta, 0.001)
    pipeline.done
  }

  @Test def testFastPageRank {
    var prev = initialInput(tempDir.copyResourceFileName("urls.txt"))
    var delta = 1.0f
    while (delta > 0.01f) {
      prev = fastUpdate(prev, Array(0.5f))
      delta = prev.values.map(_.delta).max.value()
    }
    assertEquals(0.0048, delta, 0.001)
    pipeline.done
  }
}
