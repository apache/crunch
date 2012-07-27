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
package org.apache.scrunch

import org.apache.crunch.io.{From => from, To => to}

import org.scalatest.junit.JUnitSuite
import _root_.org.junit.Test

class TopTest extends ScrunchTestSupport with JUnitSuite {

  @Test def topInMem {
    val ptable = Mem.tableOf(("foo", 17), ("bar", 29), ("baz", 1729))
    assert(ptable.top(1, true).materialize.head == ("baz", 1729))
  }

  @Test def top2 {
    val pipeline = Pipeline.mapReduce[TopTest](tempDir.getDefaultConfiguration)
    val input = tempDir.copyResourceFileName("shakes.txt")

    val wc = pipeline.read(from.textFile(input))
        .flatMap(_.toLowerCase.split("\\s+"))
        .filter(!_.isEmpty()).count
    assert(wc.top(10, true).materialize.exists(_ == ("is", 205)))
  }
}
