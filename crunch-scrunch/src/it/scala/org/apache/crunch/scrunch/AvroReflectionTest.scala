/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.crunch.scrunch

import java.nio.ByteBuffer
import org.junit.Test
import org.apache.crunch.types.PType

class AvroRecord1(var ba: Array[Byte], var bbl: Array[ByteBuffer]) {
  def this() { this(null, null) }
}

class AvroReflectionTest extends CrunchSuite {

  def assertEquals[T](t: T, pt: PType[T]) = {
    t.equals(pt.getInputMapFn().map(pt.getOutputMapFn().map(t)))
  }

  @Test def testAvroRecord1 {
    val pt = Avros.reflects[AvroRecord1]
    val r = new AvroRecord1(Array[Byte](127),
      Array[ByteBuffer](ByteBuffer.wrap(Array[Byte](4, 13, 12)), ByteBuffer.wrap(Array[Byte](13, 14, 10))))
    assertEquals(r, pt)
  }

  @Test def runAvroRecord1 {
    val shakes = tempDir.copyResourceFileName("shakes.txt")
    val p = Pipeline.mapReduce(classOf[AvroReflectionTest], tempDir.getDefaultConfiguration)
    p.read(From.textFile(shakes, Avros.strings))
      .map(x => new AvroRecord1(Array[Byte](1), Array[ByteBuffer](ByteBuffer.wrap(Array[Byte](2)))))
      .by(x => 1)
      .groupByKey(1)
      .ungroup()
      .write(To.avroFile(tempDir.getFileName("out")))
    p.done()
  }
}
