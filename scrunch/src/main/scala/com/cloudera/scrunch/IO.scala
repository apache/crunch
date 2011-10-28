
package com.cloudera.scrunch

import com.cloudera.crunch.{Source, SourceTarget, Target}
import com.cloudera.crunch.io.{From => from, To => to, At => at}
import com.cloudera.crunch.`type`.PType
import com.cloudera.crunch.`type`.avro.AvroType
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
