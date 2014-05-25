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
package org.apache.crunch.scrunch;

import org.junit.Test;

object ScalaTypesTest {
  def et(s: String): Either[String, Int] = {
    if (s.startsWith("a")) {
      Left(s)
    } else {
      Right(s.length)
    }
  }
}

class ScalaTypesTest extends CrunchSuite {
  import ScalaTypesTest._

  lazy val pipeline = Pipeline.mapReduce[ScalaTypesTest](tempDir.getDefaultConfiguration)

  @Test
  def option {
    val shakespeare = tempDir.copyResourceFileName("shakes.txt")

    val out = pipeline.read(From.textFile(shakespeare))
        .map(x => if (x.startsWith("a")) Some(x) else None)
        .materialize
        .take(100)
    pipeline.done
    assert(out.exists(!_.isEmpty))
  }

  @Test
  def either {
    val shakespeare = tempDir.copyResourceFileName("shakes.txt")

    val out = pipeline.read(From.textFile(shakespeare))
      .map(et)
      .materialize
      .take(100)
    pipeline.done
    assert(out.exists(_.isLeft))
    assert(out.exists(_.isRight))
  }
}
