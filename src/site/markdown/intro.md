<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
# Introduction to Apache Crunch
---

## Build and Installation

To use Crunch you first have to build the source code using Maven and install
it in your local repository:

    mvn clean install

This also runs the integration test suite which will take a while. Afterwards
you can run the bundled example applications:

    hadoop jar examples/target/crunch-examples-*-job.jar org.apache.crunch.examples.WordCount <inputfile> <outputdir>


## High Level Concepts

### Data Model and Operators

Crunch is centered around three interfaces that represent distributed datasets: `PCollection<T>`, `PTable<K, V>`, and `PGroupedTable<K, V>`.

A `PCollection<T>` represents a distributed, unordered collection of elements of type T. For example, we represent a text file in Crunch as a
`PCollection<String>` object. PCollection provides a method, `parallelDo`, that applies a function to each element in a PCollection in parallel,
and returns a new PCollection as its result.

A `PTable<K, V>` is a sub-interface of PCollection that represents a distributed, unordered multimap of its key type K to its value type V.
In addition to the parallelDo operation, PTable provides a `groupByKey` operation that aggregates all of the values in the PTable that
have the same key into a single record. It is the groupByKey operation that triggers the sort phase of a MapReduce job.

The result of a groupByKey operation is a `PGroupedTable<K, V>` object, which is a distributed, sorted map of keys of type K to an Iterable
collection of values of type V. In addition to parallelDo, the PGroupedTable provides a `combineValues` operation, which allows for
a commutative and associative aggregation operator to be applied to the values of the PGroupedTable instance on both the map side and the
reduce side of a MapReduce job.

Finally, PCollection, PTable, and PGroupedTable all support a `union` operation, which takes a series of distinct PCollections and treats
them as a single, virtual PCollection. The union operator is required for operations that combine multiple inputs, such as cogroups and
joins.

### Pipeline Building and Execution

Every Crunch pipeline starts with a `Pipeline` object that is used to coordinate building the pipeline and executing the underlying MapReduce
jobs. For efficiency, Crunch uses lazy evaluation, so it will only construct MapReduce jobs from the different stages of the pipelines when
the Pipeline object\'s `run` or `done` methods are called.

## A Detailed Example

Here is the classic WordCount application using Crunch:

    import org.apache.crunch.DoFn;
    import org.apache.crunch.Emitter;
    import org.apache.crunch.PCollection;
    import org.apache.crunch.PTable;
    import org.apache.crunch.Pipeline;
    import org.apache.crunch.impl.mr.MRPipeline;
    import org.apache.crunch.lib.Aggregate;
    import org.apache.crunch.types.writable.Writables;

    public class WordCount {
      public static void main(String[] args) throws Exception {
        Pipeline pipeline = new MRPipeline(WordCount.class);
        PCollection<String> lines = pipeline.readTextFile(args[0]);

        PCollection<String> words = lines.parallelDo("my splitter", new DoFn<String, String>() {
          public void process(String line, Emitter<String> emitter) {
            for (String word : line.split("\\s+")) {
              emitter.emit(word);
            }
          }
        }, Writables.strings());

        PTable<String, Long> counts = Aggregate.count(words);

        pipeline.writeTextFile(counts, args[1]);
        pipeline.run();
      }
    }

Let\'s walk through the example line by line.

### Step 1: Creating a Pipeline and referencing a text file

The `MRPipeline` implementation of the Pipeline interface compiles the individual stages of a
pipeline into a series of MapReduce jobs. The MRPipeline constructor takes a class argument
that is used to tell Hadoop where to find the code that is used in the pipeline execution.

We now need to tell the Pipeline about the inputs it will be consuming. The Pipeline interface
defines a `readTextFile` method that takes in a String and returns a PCollection of Strings.
In addition to text files, Crunch supports reading data from SequenceFiles and Avro container files,
via the `SequenceFileSource` and `AvroFileSource` classes defined in the org.apache.crunch.io package.

Note that each PCollection is a _reference_ to a source of data- no data is actually loaded into a
PCollection on the client machine.

### Step 2: Splitting the lines of text into words

Crunch defines a small set of primitive operations that can be composed in order to build complex data
pipelines. The first of these primitives is the `parallelDo` function, which applies a function (defined
by a subclass of `DoFn`) to every record in a PCollection, and returns a new PCollection that contains
the results.

The first argument to parallelDo is a string that is used to identify this step in the pipeline. When
a pipeline is composed into a series of MapReduce jobs, it is often the case that multiple stages will
run within the same Mapper or Reducer. Having a string that identifies each processing step is useful
for debugging errors that occur in a running pipeline.

The second argument to parallelDo is an anonymous subclass of DoFn. Each DoFn subclass must override
the `process` method, which takes in a record from the input PCollection and an `Emitter` object that
may have any number of output values written to it. In this case, our DoFn splits each lines up into
words, using a blank space as a separator, and emits the words from the split to the output PCollection.

The last argument to parallelDo is an instance of the `PType` interface, which specifies how the data
in the output PCollection is serialized. While Crunch takes advantage of Java Generics to provide
compile-time type safety, the generic type information is not available at runtime. Crunch needs to know
how to map the records stored in each PCollection into a Hadoop-supported serialization format in order
to read and write data to disk. Two serialization implementations are supported in crunch via the
`PTypeFamily` interface: a Writable-based system that is defined in the org.apache.crunch.types.writable
package, and an Avro-based system that is defined in the org.apache.crunch.types.avro package. Each
implementation provides convenience methods for working with the common PTypes (Strings, longs, bytes, etc.)
as well as utility methods for creating PTypes from existing Writable classes or Avro schemas.

### Step 3: Counting the words

Out of Crunch\'s simple primitive operations, we can build arbitrarily complex chains of operations in order
to perform higher-level operations, like aggregations and joins, that can work on any type of input data.
Let\'s look at the implementation of the `Aggregate.count` function:

    package org.apache.crunch.lib;

    import org.apache.crunch.CombineFn;
    import org.apache.crunch.MapFn;
    import org.apache.crunch.PCollection;
    import org.apache.crunch.PGroupedTable;
    import org.apache.crunch.PTable;
    import org.apache.crunch.Pair;
    import org.apache.crunch.types.PTypeFamily;

    public class Aggregate {

      private static class Counter<S> extends MapFn<S, Pair<S, Long>> {
        public Pair<S, Long> map(S input) {
              return Pair.of(input, 1L);
        }
      }

      public static <S> PTable<S, Long> count(PCollection<S> collect) {
        PTypeFamily tf = collect.getTypeFamily();

        // Create a PTable from the PCollection by mapping each element
        // to a key of the PTable with the value equal to 1L
        PTable<S, Long> withCounts = collect.parallelDo("count:" + collect.getName(),
            new Counter<S>(), tf.tableOf(collect.getPType(), tf.longs()));

        // Group the records of the PTable based on their key.
        PGroupedTable<S, Long> grouped = withCounts.groupByKey();

        // Sum the 1L values associated with the keys to get the
        // count of each element in this PCollection, and return it
        // as a PTable so that it may be processed further or written
        // out for storage.
        return grouped.combineValues(CombineFn.<S>SUM_LONGS());
      }
    }

First, we get the PTypeFamily that is associated with the PType for the collection. The
call to parallelDo converts each record in this PCollection into a Pair of the input record
and the number one by extending the `MapFn` convenience subclass of DoFn, and uses the
`tableOf` method of the PTypeFamily to specify that the returned PCollection should be a
PTable instance, with the key being the PType of the PCollection and the value being the Long
implementation for this PTypeFamily.

The next line features the second of Crunch\'s four operations, `groupByKey`. The groupByKey
operation may only be applied to a PTable, and returns an instance of the `PGroupedTable`
interface, which references the grouping of all of the values in the PTable that have the same key.
The groupByKey operation is what triggers the reduce phase of a MapReduce within Crunch.

The last line in the function returns the output of the third of Crunch\'s four operations,
`combineValues`. The combineValues operator takes a `CombineFn` as an argument, which is a
specialized subclass of DoFn that operates on an implementation of Java\'s Iterable interface. The
use of combineValues (as opposed to parallelDo) signals to Crunch that the CombineFn may be used to
aggregate values for the same key on the map side of a MapReduce job as well as the reduce side.

### Step 4: Writing the output and running the pipeline

The Pipeline object also provides a `writeTextFile` convenience method for indicating that a
PCollection should be written to a text file. There are also output targets for SequenceFiles and
Avro container files, available in the org.apache.crunch.io package.

After you are finished constructing a pipeline and specifying the output destinations, call the
pipeline\'s blocking `run` method in order to compile the pipeline into one or more MapReduce
jobs and execute them.


## More Information

[Writing Your Own Pipelines](pipelines.html)
