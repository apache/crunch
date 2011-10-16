package crunch

import com.cloudera.crunch.{PCollection => JCollection, Pipeline => JPipeline}
import com.cloudera.crunch.{Source, TableSource, Target}
import com.cloudera.crunch.impl.mr.MRPipeline

class Pipeline[R: ClassManifest]() {
  val jpipeline = new MRPipeline(classManifest[R].erasure)

  def getConfiguration() = jpipeline.getConfiguration()

  def read[T](source: Source[T]) = new PCollection[T](jpipeline.read(source))

  def read[K, V](source: TableSource[K, V]) = {
    new PTable[K, V](jpipeline.read(source))
  }

  def write(pcollect: PCollection[_], target: Target): Unit = {
    jpipeline.write(pcollect.base, target)
  }

  def run(): Unit = jpipeline.run()

  def done(): Unit = jpipeline.done()

  def readTextFile(pathName: String) = {
    new PCollection[String](jpipeline.readTextFile(pathName))
  }

  def writeTextFile[T](pcollect: PCollection[T], pathName: String): Unit = {
    jpipeline.writeTextFile(pcollect.base, pathName)
  }
}
