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

import org.apache.crunch.io.{From => from}
import org.apache.crunch.types.avro.{Avros => A}
import org.apache.avro.file.DataFileWriter
import org.apache.hadoop.fs.{Path, FSDataOutputStream}
import org.apache.hadoop.conf.Configuration

import _root_.org.junit.Assert._
import _root_.org.junit.Test
import java.nio.ByteBuffer

case class Rec1(var k: Int, var v: String) { def this() = this(0, "") }
case class Rec2(var k: Int, var k2: String, var v: Double) { def this() = this(0, "", 0.0) }
case class Rec3(var k2: String, var v:Int) { def this() = this("", 0)}

case class BBRec(var k: ByteBuffer, var ll: Array[ByteBuffer]) { def this() = this(null, null) }

object DeepCopyTest {
  def getIterator(bbr: BBRec) = new Iterator[(ByteBuffer, ByteBuffer)] {
    val nested = bbr.ll.iterator

    def hasNext() = nested.hasNext

    def next() = (bbr.k, nested.next)
  }
}

class DeepCopyTest extends CrunchSuite {
  import DeepCopyTest._

  lazy val pipe = Pipeline.mapReduce[DeepCopyTest](tempDir.getDefaultConfiguration)

  @Test def runDeepCopyBB {
    val prefix = tempDir.getFileName("bytebuffers")
    val bb1 = ByteBuffer.wrap(Array[Byte](1, 2))
    val bb2 = ByteBuffer.wrap(Array[Byte](3, 4))
    val bb3 = ByteBuffer.wrap(Array[Byte](5, 6))
    val bb4 = ByteBuffer.wrap(Array[Byte](7, 8))

    val ones = Seq(BBRec(bb1, Array(bb4, bb2)), BBRec(bb2, Array(bb1, bb3)))
    val twos = Seq(BBRec(bb3, Array(bb1, bb2)), BBRec(bb4, Array(bb3, bb4)))
    writeCollection(new Path(prefix + "/ones"), ones)
    writeCollection(new Path(prefix + "/twos"), twos)

    val oneF = pipe.read(from.avroFile(prefix + "/ones", Avros.reflects[BBRec]))
    val twoF = pipe.read(from.avroFile(prefix + "/twos", Avros.reflects[BBRec]))

    val m = oneF.flatMap(getIterator(_)).leftJoin(twoF.flatMap(getIterator(_)))
      .keys
      .materialize
    assert(m.size == 4)
    pipe.done()
  }

  @Test def runDeepCopy {
    val prefix = tempDir.getFileName("isolation")

    val ones = Seq(Rec1(1, "hello"), Rec1(1, "tjena"), Rec1(2, "goodbye"))
    val twos = Seq(Rec2(1, "a", 0.4), Rec2(1, "a", 0.5), Rec2(1, "b", 0.6), Rec2(1, "b", 0.7), Rec2(2, "c", 9.9))
    val threes = Seq(Rec3("a", 4), Rec3("b", 5), Rec3("c", 6))

    writeCollection(new Path(prefix + "/ones"), ones)
    writeCollection(new Path(prefix + "/twos"), twos)
    writeCollection(new Path(prefix + "/threes"), threes)

    val oneF = pipe.read(from.avroFile(prefix + "/ones", A.reflects(classOf[Rec1])))
    val twoF = pipe.read(from.avroFile(prefix + "/twos", A.reflects(classOf[Rec2])))
    val threeF = pipe.read(from.avroFile(prefix + "/threes", A.reflects(classOf[Rec3])))
    val res = (oneF.by(_.k)
      cogroup
      (twoF.by(_.k2)
         innerJoin threeF.by(_.k2))
        .values()
        .by(_._1.k))
      .values()
      .materialize
      .toList

    // Expected results vs. actual
    val e12 = Set((Rec2(1, "a", 0.4), Rec3("a", 4)), (Rec2(1, "a", 0.5), Rec3("a", 4)), (Rec2(1, "b", 0.6), Rec3("b", 5)),
        (Rec2(1, "b", 0.7), Rec3("b", 5)))
    val e22 = Set((Rec2(2, "c", 9.9),Rec3("c", 6)))
    assertEquals(2, res.size)
    assertEquals(res.map(_._2.toSet), Seq(e12, e22))
    pipe.done()
  }

  private def writeCollection(path: Path, records: Iterable[_ <: AnyRef]) {
    writeAvroFile(path.getFileSystem(new Configuration()).create(path, true), records)
  }

  @SuppressWarnings(Array("rawtypes", "unchecked"))
  private def writeAvroFile[T <: AnyRef](outputStream: FSDataOutputStream, records: Iterable[T]) {
    val r: AnyRef = records.iterator.next()
    val factory = new ScalaReflectDataFactory()
    val schema = factory.getData().getSchema(r.getClass)
    val writer = factory.getWriter[T](schema)
    val dataFileWriter = new DataFileWriter(writer)
    dataFileWriter.create(schema, outputStream)

    for (record <- records) {
      dataFileWriter.append(record)
    }
    dataFileWriter.close()
    outputStream.close()
  }
}
