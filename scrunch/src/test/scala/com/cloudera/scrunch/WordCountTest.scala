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
import com.cloudera.crunch.lib.Aggregate._
import com.cloudera.scrunch.Conversions._

import org.scalatest.junit.AssertionsForJUnit
import org.junit.Assert._
import org.junit.Test

class ExampleWordTest extends AssertionsForJUnit {
  @Test def wordCount() = {
    val pipeline = new Pipeline[ExampleWordTest]
    pipeline.read(from.textFile("/tmp/shakes.txt"))
        .flatMap(_.split("\\s+")).count
        .write(to.textFile("/tmp/wc")) // Word counts
        .map((w, c) => (w.slice(0, 1), c))
        .groupByKey().combine(v => v.sum)
        .write(to.textFile("/tmp/cc")) // First char counts
    pipeline.done()
  }
}
