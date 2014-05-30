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

import org.apache.crunch.io.{From => from, To => to, At => at}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.crunch.types.PType
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.io.Writable
import org.apache.avro.specific.SpecificRecord
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

object From {
  import scala.collection.JavaConversions._

  def formattedFile[K <: Writable, V <: Writable](pathName: String, formatClass: Class[_ <: FileInputFormat[K, V]],
                          keyClass: Class[K], valueClass: Class[V]) = {
    from.formattedFile(pathName, formatClass, keyClass, valueClass)
  }

  def formattedFile[K <: Writable, V <: Writable](path: Path, formatClass: Class[_ <: FileInputFormat[K, V]],
                          keyClass: Class[K], valueClass: Class[V]) = {
    from.formattedFile(path, formatClass, keyClass, valueClass)
  }

  def formattedFile[K <: Writable, V <: Writable](paths: List[Path], formatClass: Class[_ <: FileInputFormat[K, V]],
                          keyClass: Class[K], valueClass: Class[V]) = {
    from.formattedFile(paths, formatClass, keyClass, valueClass)
  }

  def formattedFile[K, V](pathName: String, formatClass: Class[_ <: FileInputFormat[_, _]],
                          keyType: PType[K], valueType: PType[V]) = {
    from.formattedFile(pathName, formatClass, keyType, valueType)
  }

  def formattedFile[K, V](path: Path, formatClass: Class[_ <: FileInputFormat[_, _]],
                          keyType: PType[K], valueType: PType[V]) = {
    from.formattedFile(path, formatClass, keyType, valueType)
  }

  def formattedFile[K, V](paths: List[Path], formatClass: Class[_ <: FileInputFormat[_, _]],
                          keyType: PType[K], valueType: PType[V]) = {
    from.formattedFile(paths, formatClass, keyType, valueType)
  }

  def avroFile[T <: SpecificRecord](pathName: String, avroClass: Class[T]) = {
    from.avroFile(pathName, avroClass)
  }

  def avroFile[T <: SpecificRecord](path: Path, avroClass: Class[T]) = {
    from.avroFile(path, avroClass)
  }

  def avroFile[T <: SpecificRecord](paths: List[Path], avroClass: Class[T]) = {
    from.avroFile(paths, avroClass)
  }

  def avroFile[T](pathName: String, ptype: PType[T]) = {
    from.avroFile(pathName, ptype)
  }

  def avroFile[T](path: Path, ptype: PType[T]) = {
    from.avroFile(path, ptype)
  }

  def avroFile[T](paths: List[Path], ptype: PType[T]) = {
    from.avroFile(paths, ptype)
  }

  def avroFile(pathName: String) = {
    from.avroFile(pathName)
  }

  def avroFile(path: Path) = {
    from.avroFile(path)
  }

  def avroFile(paths: List[Path]) = {
    from.avroFile(paths)
  }

  def avroFile(path: Path, conf: Configuration) = {
    from.avroFile(path, conf)
  }

  def avroFile(paths: List[Path], conf: Configuration) = {
    from.avroFile(paths, conf)
  }

  def sequenceFile[T <: Writable](pathName: String, valueClass: Class[T]) = {
    from.sequenceFile(pathName, valueClass)
  }

  def sequenceFile[T <: Writable](path: Path, valueClass: Class[T]) = {
    from.sequenceFile(path, valueClass)
  }

  def sequenceFile[T <: Writable](paths: List[Path], valueClass: Class[T]) = {
    from.sequenceFile(paths, valueClass)
  }

  def sequenceFile[T](pathName: String, ptype: PType[T]) = {
    from.sequenceFile(pathName, ptype)
  }

  def sequenceFile[T](path: Path, ptype: PType[T]) = {
    from.sequenceFile(path, ptype)
  }

  def sequenceFile[T](paths: List[Path], ptype: PType[T]) = {
    from.sequenceFile(paths, ptype)
  }

  def sequenceFile[K <: Writable, V <: Writable](pathName: String, keyClass: Class[K], valueClass: Class[V]) = {
    from.sequenceFile(pathName, keyClass, valueClass)
  }

  def sequenceFile[K <: Writable, V <: Writable](path: Path, keyClass: Class[K], valueClass: Class[V]) = {
    from.sequenceFile(path, keyClass, valueClass)
  }

  def sequenceFile[K <: Writable, V <: Writable](paths: List[Path], keyClass: Class[K], valueClass: Class[V]) = {
    from.sequenceFile(paths, keyClass, valueClass)
  }

  def sequenceFile[K, V](pathName: String, keyType: PType[K], valueType: PType[V]) = {
    from.sequenceFile(pathName, keyType, valueType)
  }

  def sequenceFile[K, V](path: Path, keyType: PType[K], valueType: PType[V]) = {
    from.sequenceFile(path, keyType, valueType)
  }

  def sequenceFile[K, V](paths: List[Path], keyType: PType[K], valueType: PType[V]) = {
    from.sequenceFile(paths, keyType, valueType)
  }

  def textFile(pathName: String) = from.textFile(pathName)

  def textFile(path: Path) = from.textFile(path)

  def textFile(paths: List[Path]) = from.textFile(paths)

  def textFile[T](pathName: String, ptype: PType[T]) = from.textFile(pathName, ptype)

  def textFile[T](path: Path, ptype: PType[T]) = from.textFile(path, ptype)

  def textFile[T](paths: List[Path], ptype: PType[T]) = from.textFile(paths, ptype)
}

object To {
  def formattedFile[K <: Writable, V <: Writable](pathName: String, formatClass: Class[_ <: FileOutputFormat[K, V]]) = {
    to.formattedFile(pathName, formatClass)
  }

  def formattedFile[K <: Writable, V <: Writable](path: Path, formatClass: Class[_ <: FileOutputFormat[K, V]]) = {
    to.formattedFile(path, formatClass)
  }

  def avroFile(pathName: String) = to.avroFile(pathName)

  def avroFile(path: Path) = to.avroFile(path)

  def sequenceFile(pathName: String) = to.sequenceFile(pathName)

  def sequenceFile(path: Path) = to.sequenceFile(path)

  def textFile(pathName: String) = to.textFile(pathName)

  def textFile(path: Path) = to.textFile(path)
}

object At {
  def avroFile[T <: SpecificRecord](pathName: String, avroClass: Class[T]) = {
    at.avroFile(pathName, avroClass)
  }

  def avroFile[T <: SpecificRecord](path: Path, avroClass: Class[T]) = {
    at.avroFile(path, avroClass)
  }

  def avroFile[T](pathName: String, avroType: PType[T]) = {
    at.avroFile(pathName, avroType)
  }

  def avroFile[T](path: Path, avroType: PType[T]) = {
    at.avroFile(path, avroType)
  }

  def avroFile(pathName: String) = {
    at.avroFile(pathName)
  }

  def avroFile(path: Path) = {
    at.avroFile(path)
  }

  def avroFile(path: Path, conf: Configuration) = {
    at.avroFile(path, conf)
  }

  def sequenceFile[T <: Writable](pathName: String, valueClass: Class[T]) = {
    at.sequenceFile(pathName, valueClass)
  }

  def sequenceFile[T <: Writable](path: Path, valueClass: Class[T]) = {
    at.sequenceFile(path, valueClass)
  }

  def sequenceFile[T](pathName: String, ptype: PType[T]) = {
    at.sequenceFile(pathName, ptype)
  }

  def sequenceFile[T](path: Path, ptype: PType[T]) = {
    at.sequenceFile(path, ptype)
  }

  def sequenceFile[K <: Writable, V <: Writable](pathName: String, keyClass: Class[K], valueClass: Class[V]) = {
    at.sequenceFile(pathName, keyClass, valueClass)
  }

  def sequenceFile[K <: Writable, V <: Writable](path: Path, keyClass: Class[K], valueClass: Class[V]) = {
    at.sequenceFile(path, keyClass, valueClass)
  }

  def sequenceFile[K, V](pathName: String, keyType: PType[K], valueType: PType[V]) = {
    at.sequenceFile(pathName, keyType, valueType)
  }

  def sequenceFile[K, V](path: Path, keyType: PType[K], valueType: PType[V]) = {
    at.sequenceFile(path, keyType, valueType)
  }

  def textFile(pathName: String) = at.textFile(pathName)

  def textFile(path: Path) = at.textFile(path)

  def textFile[T](pathName: String, ptype: PType[T]) = at.textFile(pathName, ptype)

  def textFile[T](path: Path, ptype: PType[T]) = at.textFile(path, ptype)

}

