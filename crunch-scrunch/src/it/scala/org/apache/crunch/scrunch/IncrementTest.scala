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
package org.apache.crunch.scrunch

import org.apache.crunch.io.{From => from, To => to}

import _root_.org.junit.Test
import _root_.org.junit.Assert.assertEquals


class IncrementTest extends CrunchSuite {

  object Inc extends Enumeration {
    type Inc = Value
    val A, B, C, D = Value
  }
  import Inc._

  @Test def testIncrement {
    val pipeline = Pipeline.mapReduce[IncrementTest](tempDir.getDefaultConfiguration)
    val input = tempDir.copyResourceFileName("shakes.txt")

    pipeline.read(from.textFile(input, Avros.strings))
        .flatMap(_.toLowerCase.split("\\s+"))
        .increment("TOP", "ALLWORDS")
        .filter(!_.isEmpty())
        .increment("TOP", "NONEMPTY")
        .incrementIf(_ startsWith "a")("TOP", "AWORDS_2x", 2)
        .increment(Inc, Inc.A)
        .write(to.avroFile(tempDir.getFileName("somewords")))

    val res = pipeline.done()
    val sr0 = res.getStageResults.get(0)
    assertEquals(19082, sr0.getCounterValue("TOP", "ALLWORDS"))
    assertEquals(17737, sr0.getCounterValue("TOP", "NONEMPTY"))
    assertEquals(3088, sr0.getCounterValue("TOP", "AWORDS_2x"))
    assertEquals(17737, sr0.getCounterValue("Inc", "A"))
  }
}
