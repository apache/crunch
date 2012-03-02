# Crunch - Simple and Efficient Java Library for MapReduce Pipelines

## Introduction

Crunch is a Java library for writing, testing, and running MapReduce pipelines, based on
Google's FlumeJava. Its goal is to make pipelines that are composed of many user-defined
functions simple to write, easy to test, and efficient to run.

## Build and Installation

Crunch uses Maven for dependency management. The code in the examples/ subdirectory relies
on the top-level crunch libraries. In order to execute the included WordCount application, run:

	mvn install
	cd examples/
	mvn package
	hadoop jar target/crunch-examples-0.2.0-job.jar com.cloudera.crunch.examples.WordCount <inputfile> <outputdir>

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
the Pipeline object's `run` or `done` methods are called.

## A Detailed Example

Here is the classic WordCount application using Crunch:

	import com.cloudera.crunch.DoFn;
	import com.cloudera.crunch.Emitter;
	import com.cloudera.crunch.PCollection;
	import com.cloudera.crunch.PTable;
	import com.cloudera.crunch.Pipeline;
	import com.cloudera.crunch.impl.mr.MRPipeline;
	import com.cloudera.crunch.lib.Aggregate;
	import com.cloudera.crunch.type.writable.Writables;

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

Let's walk through the example line by line.

### Step 1: Creating a Pipeline and referencing a text file

The `MRPipeline` implementation of the Pipeline interface compiles the individual stages of a
pipeline into a series of MapReduce jobs. The MRPipeline constructor takes a class argument
that is used to tell Hadoop where to find the code that is used in the pipeline execution.

We now need to tell the Pipeline about the inputs it will be consuming. The Pipeline interface
defines a `readTextFile` method that takes in a String and returns a PCollection of Strings.
In addition to text files, Crunch supports reading data from SequenceFiles and Avro container files,
via the `SequenceFileSource` and `AvroFileSource` classes defined in the com.cloudera.crunch.io package.

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
`PTypeFamily` interface: a Writable-based system that is defined in the com.cloudera.crunch.type.writable
package, and an Avro-based system that is defined in the com.cloudera.crunch.type.avro package. Each
implementation provides convenience methods for working with the common PTypes (Strings, longs, bytes, etc.)
as well as utility methods for creating PTypes from existing Writable classes or Avro schemas.

### Step 3: Counting the words

Out of Crunch's simple primitive operations, we can build arbitrarily complex chains of operations in order
to perform higher-level operations, like aggregations and joins, that can work on any type of input data.
Let's look at the implementation of the `Aggregate.count` function:

	package com.cloudera.crunch.lib;

	import com.cloudera.crunch.CombineFn;
	import com.cloudera.crunch.MapFn;
	import com.cloudera.crunch.PCollection;
	import com.cloudera.crunch.PTable;
	import com.cloudera.crunch.Pair;
	import com.cloudera.crunch.type.PTypeFamily;
	
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

The next line features the second of Crunch's four operations, `groupByKey`. The groupByKey
operation may only be applied to a PTable, and returns an instance of the `PGroupedTable`
interface, which references the grouping of all of the values in the PTable that have the same key.
The groupByKey operation is what triggers the reduce phase of a MapReduce within Crunch.

The last line in the function returns the output of the third of Crunch's four operations,
`combineValues`. The combineValues operator takes a `CombineFn` as an argument, which is a
specialized subclass of DoFn that operates on an implementation of Java's Iterable interface. The
use of combineValues (as opposed to parallelDo) signals to Crunch that the CombineFn may be used to
aggregate values for the same key on the map side of a MapReduce job as well as the reduce side.

### Step 4: Writing the output and running the pipeline

The Pipeline object also provides a `writeTextFile` convenience method for indicating that a
PCollection should be written to a text file. There are also output targets for SequenceFiles and
Avro container files, available in the com.cloudera.crunch.io package.

After you are finished constructing a pipeline and specifying the output destinations, call the
pipeline's blocking `run` method in order to compile the pipeline into one or more MapReduce
jobs and execute them.

## Writing Your Own Pipelines

This section discusses the different steps of creating your own Crunch pipelines in more detail.

### Writing a DoFn

The DoFn class is designed to keep the complexity of the MapReduce APIs out of your way when you
don't need them while still keeping them accessible when you do.

#### Serialization

First, all DoFn instances are required to be `java.io.Serializable`. This is a key aspect of Crunch's design:
once a particular DoFn is assigned to the Map or Reduce stage of a MapReduce job, all of the state
of that DoFn is serialized so that it may be distributed to all of the nodes in the Hadoop cluster that
will be running that task. There are two important implications of this for developers:

1. All member values of a DoFn must be either serializable or marked as `transient`.
2. All anonymous DoFn instances must be defined in a static method or in a class that is itself serializable.

Because sometimes you will need to work with non-serializable objects inside of a DoFn, every DoFn provides an
`initialize` method that is called before the `process` method is ever called so that any initialization tasks,
such as creating a non-serializable member variable, can be performed before processing begins. Similarly, all
DoFn instances have a `cleanup` method that may be called after processing has finished to perform any required
cleanup tasks.

#### Scale Factor

The DoFn class defines a `scaleFactor` method that can be used to signal to the MapReduce compiler that a particular
DoFn implementation will yield an output PCollection that is larger (scaleFactor > 1) or smaller (0 < scaleFactor < 1)
than the input PCollection it is applied to. The compiler may use this information to determine how to optimally
split processing tasks between the Map and Reduce phases of dependent MapReduce jobs.

#### Other Utilities

The DoFn base class provides convenience methods for accessing the `Configuration` and `Counter` objects that
are associated with a MapReduce stage, so that they may be accessed during initialization, processing, and cleanup.

### Performing Cogroups and Joins

In Crunch, cogroups and joins are performed on PTable instances that have the same key type. This section walks through
the basic flow of a cogroup operation, explaining how this higher-level operation is composed of Crunch's four primitives.
In general, these common operations are provided as part of the core Crunch library or in extensions, you do not need
to write them yourself. But it can be useful to understand how they work under the covers.

Assume we have a `PTable<K, U>` named "a" and a different `PTable<K, V>` named "b" that we would like to combine into a
single `PTable<K, Pair<Collection<U>, Collection<V>>>`. First, we need to apply parallelDo operations to a and b that
convert them into the same Crunch type, `PTable<K, Pair<U, V>>`:

	// Perform the "tagging" operation as a parallelDo on PTable a
	PTable<K, Pair<U, V>> aPrime = a.parallelDo("taga", new MapFn<Pair<K, U>, Pair<K, Pair<U, V>>>() {
	  public Pair<K, Pair<U, V>> map(Pair<K, U> input) {
	    return Pair.of(input.first(), Pair.of(input.second(), null));
	  }
	}, tableOf(a.getKeyType(), pair(a.getValueType(), b.getValueType())));
	
	// Perform the "tagging" operation as a parallelDo on PTable b
	PTable<K, Pair<U, V>> bPrime = b.parallelDo("tagb", new MapFn<Pair<K, V>, Pair<K, Pair<U, V>>>() {
	  public Pair<K, Pair<U, V>> map(Pair<K, V> input) {
	    return Pair.of(input.first(), Pair.of(null, input.second()));
	  }
	}, tableOf(a.getKeyType(), pair(a.getValueType(), b.getValueType())));

Once the input PTables are tagged into a single type, we can apply the union operation to create a single PTable
reference that includes both of the tagged PTables and then group the unioned PTable by the common key:

	PTable<K, Pair<U, V>> both = aPrime.union(bPrime);
	PGroupedTable<K, Pair<U, V>> grouped = both.groupByKey();

The grouping operation will create an `Iterable<Pair<U, V>>` which we can then convert to a `Pair<Collection<U>, Collection<V>>`:

	grouped.parallelDo("cogroup", new MapFn<Pair<K, Iterable<Pair<U, V>>>, Pair<K, Pair<Collection<U>, Collection<V>>>>() {
	  public Pair<K, Pair<Collection<U>, Collection<V>>> map(Pair<K, Iterable<Pair<U, V>>> input) {
	    Collection<U> uValues = new ArrayList<U>();
	    Collection<V> vValues = new ArrayList<V>();
	    for (Pair<U, V> pair : input.second()) {
	      if (pair.first() != null) {
	        uValues.add(pair.first());
	      } else {
	        vValues.add(pair.second());
	      }
	    }
	    return Pair.of(input.first(), Pair.of(uValues, vValues));
	  },
	}, tableOf(grouped.getKeyType(), pair(collections(a.getValueType()), collections(b.getValueType()))));

## Current Limitations and Future Work

This section contains an almost certainly incomplete list of known limitations of Crunch and plans for future work.

* We would like to have easy support for reading and writing data from/to HCatalog.
* The decision of how to split up processing tasks between dependent MapReduce jobs is very naiive right now- we simply
delegate all of the work to the reduce stage of the predecessor job. We should take advantage of information about the
expected size of different PCollections to optimize this processing.
* The Crunch optimizer does not yet merge different groupByKey operations that run over the same input data into a single
MapReduce job. Implementing this optimization will provide a major performance benefit for a number of problems.
