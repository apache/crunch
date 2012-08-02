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
# Writing Your Own Pipelines
---

This section discusses the different steps of creating your own Crunch pipelines in more detail.

## Writing a DoFn

The DoFn class is designed to keep the complexity of the MapReduce APIs out of your way when you
don\'t need them while still keeping them accessible when you do.

### Serialization

First, all DoFn instances are required to be `java.io.Serializable`. This is a key aspect of Crunch\'s design:
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

### Scale Factor

The DoFn class defines a `scaleFactor` method that can be used to signal to the MapReduce compiler that a particular
DoFn implementation will yield an output PCollection that is larger (scaleFactor > 1) or smaller (0 < scaleFactor < 1)
than the input PCollection it is applied to. The compiler may use this information to determine how to optimally
split processing tasks between the Map and Reduce phases of dependent MapReduce jobs.

### Other Utilities

The DoFn base class provides convenience methods for accessing the `Configuration` and `Counter` objects that
are associated with a MapReduce stage, so that they may be accessed during initialization, processing, and cleanup.

### Performing Cogroups and Joins

In Crunch, cogroups and joins are performed on PTable instances that have the same key type. This section walks through
the basic flow of a cogroup operation, explaining how this higher-level operation is composed of Crunch\'s four primitives.
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
