package crunch

import com.cloudera.crunch.{PCollection => JCollection, Pipeline => JPipeline}
import com.cloudera.crunch.{Source, TableSource, Target}
import com.cloudera.crunch.impl.mr.MRPipeline
import org.apache.hadoop.conf.Configuration

class Pipeline[R: ClassManifest](conf: Configuration = new Configuration()) {
  val jpipeline = new MRPipeline(classManifest[R].erasure, conf)

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
