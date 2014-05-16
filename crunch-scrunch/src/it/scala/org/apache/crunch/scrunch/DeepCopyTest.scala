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

case class Rec1(var k: Int, var v: String) { def this() = this(0, "") }
case class Rec2(var k: Int, var k2: String, var v: Double) { def this() = this(0, "", 0.0) }
case class Rec3(var k2: String, var v:Int) { def this() = this("", 0)}

class DeepCopyTest extends CrunchSuite {
  lazy val pipe = Pipeline.mapReduce[DeepCopyTest](tempDir.getDefaultConfiguration)

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
    val schema = new ScalaReflectDataFactory().getReflectData.getSchema(r.getClass)

    val writer = new ScalaReflectDataFactory().getWriter[T](schema)
    val dataFileWriter = new DataFileWriter(writer)
    dataFileWriter.create(schema, outputStream)

    for (record <- records) {
      dataFileWriter.append(record)
    }
    dataFileWriter.close()
    outputStream.close()
  }
}
