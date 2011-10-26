# Scrunch - A Scala Wrapper for Crunch

## Introduction

Scrunch is an experimental Scala wrapper for Crunch, based on the same ideas as the
[Cascade](http://days2011.scala-lang.org/node/138/282) project at Google, which created
a Scala wrapper for FlumeJava.

## Why Scala?

In many ways, Scala is the perfect language for writing Crunch pipelines. Scala supports
a mixture of functional and object-oriented programming styles and has powerful type-inference
capabilities, allowing us to create complex pipelines using very few keystrokes. Here is
the Scrunch analogue of the classic WordCount problem:

	import com.cloudera.crunch.io.{From => from}
	import com.cloudera.scrunch._
	import com.cloudera.scrunch.Conversions_  # For implicit type conversions

	class WordCountExample {
	  val pipeline = new Pipeline[WordCountExample]

	  def wordCount(fileName: String) = {
	    pipeline.read(from.textFile(fileName))
	      .flatMap(_.toLowerCase.split("\\W+"))
	      .filter(!_.isEmpty())
	      .count
	  }
	}

The Scala compiler can infer the return type of the flatMap function as an Array[String], and
the Scrunch wrapper uses the type inference mechanism to figure out how to serialize the
data between the Map and Reduce stages. Here's a slightly more complex example, in which we
get the word counts for two different files and compute the deltas of how often different
words occur, and then only returns the words where the first file had more occurrences then
the second:

	class WordCountExample {
	  def wordGt(firstFile: String, secondFile: String) = {
	    wordCount(firstFile).cogroup(wordCount(secondFile))
	      .map((k, v) => (k, (v._1.sum - v._2.sum)))
	      .filter((k, v) => v > 0).map((k, v) => k)
	  }
	}

Note that all of the functions are using Scala Tuples, not Crunch Tuples. Under the covers,
Scrunch uses Scala's implicit type conversion mechanism to transparently convert data from the
Crunch format to the Scala format and back again.

## Materializing Job Outputs

Scrunch also incorporates Crunch's materialize functionality, which allows us to easily read
the output of a Crunch pipeline into the client:

	class WordCountExample {
	  def hasHamlet = wordGt("shakespeare.txt", "maugham.txt").materialize.exists(_ == "hamlet")
	}

## Notes and Thanks

Scrunch is alpha-quality code, written by someone who was learning Scala on the fly. There will be bugs,
rough edges, and non-idiomatic Scala usage all over the place. This will improve with time, and we welcome
contributions from Scala experts who are interested in helping us make Scrunch into a first-class project.
The Crunch developers mailing list is [here](https://groups.google.com/a/cloudera.org/group/crunch-dev/topics).

Scrunch emerged out of conversations with [Dmitriy Ryaboy](http://twitter.com/#!/squarecog),
[Oscar Boykin](http://twitter.com/#!/posco), and [Avi Bryant](http://twitter.com/#!/avibryant) from Twitter.
Many thanks to them for their feedback, guidance, and encouragement. We are also grateful to
[Matei Zaharia](http://twitter.com/#!/matei_zaharia), whose [Spark Project](http://www.spark-project.org/)
inspired much of our implementation and was kind enough to loan us the ClosureCleaner implementation
Spark developed for use in Scrunch.
