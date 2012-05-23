/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
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

import com.cloudera.crunch.io.{From => from, To => to, At => at}
import com.cloudera.crunch.types.avro.AvroType
import org.apache.hadoop.fs.Path;

object From {
  def avroFile[T](path: String, atype: AvroType[T]) = from.avroFile(path, atype)
  def avroFile[T](path: Path, atype: AvroType[T]) = from.avroFile(path, atype)
  def textFile(path: String) = from.textFile(path)
  def textFile(path: Path) = from.textFile(path)
}

object To {
  def avroFile[T](path: String) = to.avroFile(path)
  def avroFile[T](path: Path) = to.avroFile(path)
  def textFile(path: String) = to.textFile(path)
  def textFile(path: Path) = to.textFile(path)
}

object At {
  def avroFile[T](path: String, atype: AvroType[T]) = at.avroFile(path, atype)
  def avroFile[T](path: Path, atype: AvroType[T]) = at.avroFile(path, atype)
  def textFile(path: String) = at.textFile(path)
  def textFile(path: Path) = at.textFile(path)
}
