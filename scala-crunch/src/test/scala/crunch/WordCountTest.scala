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
    val uc = wordCount.map2((k: String, c: JLong) => ((k.substring(0, 1), 1), c.longValue()))
    val cc = uc.groupByKey().combine((v: Iterable[Long]) => v.sum)
        .map2((k: (String, Int), v: Long) => (k._1.toUpperCase, v))
    pipeline.writeTextFile(cc, "/tmp/cc")
    pipeline.done()
  }
}
