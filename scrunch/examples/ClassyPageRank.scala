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
import com.cloudera.scrunch._
import com.cloudera.scrunch.Mem._

case class UrlData(pageRank: Float, oldPageRank: Float, links: List[String]) {
  def this() = this(1.0f, 0.0f, Nil)

  def this(links: String*) = this(1.0f, 0.0f, List(links:_*))
 
  def this(links: Iterable[String]) = this(1.0f, 0.0f, links.toList)
  
  def delta = math.abs(pageRank - oldPageRank)

  def next(newPageRank: Float) = new UrlData(newPageRank, pageRank, links)

  def outboundScores = links.map(link => (link, pageRank / links.size))
}

object ClassyPageRank extends PipelineApp {

  def initialize(file: String) = {
    read(from.textFile(file))
      .map(line => { val urls = line.split("\\s+"); (urls(0), urls(2)) })
      .groupByKey
      .map((url, links) => (url, new UrlData(links)))
  }

  def update(prev: PTable[String, UrlData], d: Float) = {
    val outbound = prev.values.flatMap(_.outboundScores)

    cogroup(prev, outbound).mapValues(data => {
      val (prd, outboundScores) = data
      val newPageRank = (1 - d) + d * outboundScores.sum
      if (!prd.isEmpty) {
        prd.head.next(newPageRank)
      } else {
        new UrlData(newPageRank, 0, Nil)
      }
    })
  }

  var index = 0
  var delta = 10.0f
  fs.mkdirs("prank/")
  var curr = initialize(args(0))
  while (delta > 1.0f) {
    index = index + 1
    curr = update(curr, 0.5f)
    write(curr, to.avroFile("prank/" + index))
    delta = curr.values.map(_.delta).max.materialize.head
    println("Current delta = " + delta)
  }
  fs.rename("prank/" + index, args(1))
  fs.delete("prank/", true)
}
