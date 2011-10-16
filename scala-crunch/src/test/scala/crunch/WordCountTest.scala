package crunch

import com.cloudera.crunch.impl.mr.MRPipeline
import com.cloudera.crunch.lib.Aggregate

import org.scalatest.junit.AssertionsForJUnit
import org.junit.Assert._
import org.junit.Test

class ExampleWordTest {
  @Test def wordCount() = {
    val pipeline = new Pipeline(new MRPipeline(classOf[ExampleWordTest]))
    val shakes = pipeline.readTextFile("/tmp/shakes.txt")
    val counts = Aggregate.count(shakes.flatMap((line: String) => line.split("\\s+")))
    pipeline.writeTextFile(counts, "/tmp/wc")
    pipeline.done()
  }
}
