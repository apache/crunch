package crunch

import com.cloudera.crunch.lib.Aggregate

import org.scalatest.junit.AssertionsForJUnit
import org.junit.Assert._
import org.junit.Test

class ExampleWordTest extends AssertionsForJUnit {
  @Test def wordCount() = {
    val pipeline = new Pipeline[ExampleWordTest]()
    val shakes = pipeline.readTextFile("/tmp/shakes.txt")
    val counts = Aggregate.count(shakes.flatMap(_.split("\\s+")))
    pipeline.writeTextFile(counts, "/tmp/wc")
    pipeline.done()
  }
}
