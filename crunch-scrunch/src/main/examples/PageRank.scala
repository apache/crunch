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
import org.apache.crunch.scrunch._

object PageRank extends PipelineApp {
  def initialize(file: String) = {
    read(from.textFile(file))
      .map(line => { val urls = line.split("\\s+"); (urls(0), urls(2)) })
      .groupByKey
      .map((url, links) => (url, (1f, 0f, links.toList)))
  }

  def update(prev: PTable[String, (Float, Float, List[String])], d: Float) = {
    val outbound = prev.flatMap((url, data) => {
      val (pagerank, old_pagerank, links) = data
      links.map(link => (link, pagerank / links.size))
    })

    cogroup(prev, outbound).mapValues(data => {
      val (prev_data, outbound_data) = data
      val new_pagerank = (1 - d) + d * outbound_data.sum
      var cur_pagerank = 0f
      var links: List[String] = Nil
      if (!prev_data.isEmpty) {
        val (cur_pr, old_pr, l) = prev_data.head
        cur_pagerank = cur_pr
        links = l
      }
      (new_pagerank, cur_pagerank, links)
    })
  }

  override def run(args: Array[String]) {
    var index = 0
    var delta = 10.0f
    fs.mkdirs("prank/")
    var curr = initialize(args(0))
    while (delta > 1.0f) {
      index = index + 1
      curr = update(curr, 0.5f)
      write(curr, to.avroFile("prank/" + index))
      delta = curr.values.map(v => math.abs(v._1 - v._2)).max.value()
      println("Current delta = " + delta)
    }
    fs.rename("prank/" + index, args(1))
    fs.delete("prank/", true)
  }
}
