
import com.cloudera.scrunch._

object PageRank extends PipelineApp {
  def initialize(file: String) = {
    read(from.textFile(file))
      .map(line => { val urls = line.split("\\s+"); (urls(0), urls(2)) })
      .groupByKey
      .map((url, links) => (url, (1f, 0f, links.toList.toIterable)))
  }

  def update(prev: PTable[String, (Float, Float, Iterable[String])], d: Float) = {
    val outbound = prev.flatMap((url, data) => {
      val (pagerank, old_pagerank, links) = data
      links.map(link => (link, pagerank / links.size))
    })

    cogroup(prev, outbound).map((url, data) => {
      val (prev_data, outbound_data) = data
      val new_pagerank = (1 - d) + d * outbound_data.sum
      var cur_pagerank = 0f
      var links: Iterable[String] = Nil
      if (!prev_data.isEmpty) {
        val (cur_pr, old_pr, l) = prev_data.head
        cur_pagerank = cur_pr
        links = l
      }
      (url, (new_pagerank, cur_pagerank, links))
    })
  }

  var index = 0
  var delta = 10.0f
  fs.mkdirs("prank/")
  var curr = initialize(args(0))
  while (delta > 1.0f) {
    index = index + 1
    curr = update(curr, 0.5f)
    write(curr, to.avroFile("prank/" + index))
    delta = curr.values.map(v => math.abs(v._1 - v._2)).max.materialize.head
    println("Current delta = " + delta)
  }
  fs.rename("prank/" + index, args(1))
  fs.delete("prank/", true)
}
