
import com.cloudera.scrunch.PipelineApp

object WordCount extends PipelineApp {

  def countWords(file: String) = {
    read(from.textFile(file))
      .flatMap(_.split("\\W+").filter(!_.isEmpty()))
      .count
  }

  val counts = cogroup(countWords(args(0)), countWords(args(1)))
  write(counts, to.textFile(args(2)))
}
