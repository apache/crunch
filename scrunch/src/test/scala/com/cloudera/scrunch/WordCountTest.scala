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

import com.cloudera.crunch.io.{From => from, To => to}
import com.cloudera.crunch.lib.Aggregate
import com.cloudera.scrunch.Conversions._

import org.scalatest.junit.AssertionsForJUnit
import org.junit.Assert._
import org.junit.Test

class ExampleWordTest extends AssertionsForJUnit {
  @Test def wordCount() = {
    val pipeline = new Pipeline[ExampleWordTest]
    val input = pipeline.read(from.textFile("/tmp/shakes.txt"))
    val words = input.flatMap(_.split("\\s+"))
    val wordCount = Aggregate.count(words)
    pipeline.writeTextFile(wordCount, "/tmp/wc")
    val uc = wordCount.map((w, c) => ((w.substring(0, 1), w.length), c.longValue()))
    val cc = uc.groupByKey().combine(v => v.sum).map((k, v) => (k._1, v))
    pipeline.write(cc, to.textFile("/tmp/cc"))
    pipeline.done()
  }
}
