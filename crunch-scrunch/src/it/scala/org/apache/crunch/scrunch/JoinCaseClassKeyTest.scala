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

import java.nio.ByteBuffer

import _root_.org.junit.Test

case class AKey(a: String, b: ByteBuffer)
case class Keyed1(key: AKey, value: String)
case class Keyed2(key: AKey, value: Int)

class JoinCaseClassKeyTest extends CrunchSuite {

  lazy val pipeline = Pipeline.mapReduce[JoinCaseClassKeyTest](tempDir.getDefaultConfiguration)

  @Test def testCaseClassJoin: Unit = {
    val bb1 = ByteBuffer.wrap(Array[Byte](1, 2, 0))
    val bb2 = ByteBuffer.wrap(Array[Byte](1, 2, 0))
    val bb3 = ByteBuffer.wrap(Array[Byte](1, 0, 3))
    val bb4 = ByteBuffer.wrap(Array[Byte](1, 0, 3))
    val k = Array(AKey("a", bb1), AKey("a", bb2), AKey("a", bb3), AKey("a", bb4))
    val ones = Seq(Keyed1(k(0), "x"), Keyed1(k(2), "r"))
    val twos = Seq(Keyed2(k(1), 1), Keyed2(k(3), 2))

    val o = pipeline.create(ones, Avros.caseClasses[Keyed1])
    val t = pipeline.create(twos, Avros.caseClasses[Keyed2])
    val joined = o.by(x => x.key).join(t.by(x => x.key))
    assert(2 == joined.materialize().seq.size)
  }
}
