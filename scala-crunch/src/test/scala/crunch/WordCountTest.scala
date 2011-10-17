package crunch

import com.cloudera.crunch.lib.Aggregate._
import crunch.PTable._

import java.lang.{Long => JLong}
import org.scalatest.junit.AssertionsForJUnit
import org.junit.Assert._
import org.junit.Test

class ExampleWordTest extends AssertionsForJUnit {
  @Test def wordCount() = {
    val pipeline = new Pipeline[ExampleWordTest]
    val input = pipeline.readTextFile("/tmp/shakes.txt")
    val words = input.flatMap(_.split("\\s+"))
    val wordCount = count(words)
    pipeline.writeTextFile(wordCount, "/tmp/wc")
    val uc = wordCount.map2((w, c) => ((w.substring(0, 1), w.length), c.longValue()))
    val cc = uc.groupByKey().combine(v => v.sum).map2((k, v) => (k._1.toUpperCase, v))
    pipeline.writeTextFile(cc, "/tmp/cc")
    pipeline.done()
  }
}
