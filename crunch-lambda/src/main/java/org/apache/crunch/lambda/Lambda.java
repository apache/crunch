/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.crunch.lambda;

import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;

/**
 * Entry point for the crunch-lambda API. Use this to create {@link LCollection}, {@link LTable} and
 * {@link LGroupedTable} objects from their corresponding {@link PCollection}, {@link PTable} and {@link PGroupedTable}
 * types.
 *
 * <p>The crunch-lambda API allows you to write Crunch pipelines using lambda expressions and method references instead
 * of creating classes (anonymous, inner, or top level) for each operation that needs to be completed. Many pipelines
 * are composed of a large number of simple operations, rather than a small number of complex operations, making this
 * strategy much more efficient to code and easy to read for those able to use Java 8 in their distributed computation
 * environments.</p>
 *
 * <p>You use the API by wrapping your Crunch type into an L-type object. This class provides static methods for that.
 * You can then use the lambda API methods on the L-type object, yielding more L-type objects. If at any point you need
 * to go back to the standard Crunch world (for compatibility with existing code or complex use cases), you can at any
 * time call underlying() on an L-type object to get a Crunch object</p>
 *
 * <p>Example (the obligatory wordcount):</p>
 *
 * <pre>{@code
 * Pipeline pipeline = new MRPipeline(getClass());
 * LCollection<String> inputText = Lambda.wrap(pipeline.readTextFile("/path/to/input/file"));
 * inputText.flatMap(line -> Arrays.stream(line.split(" ")), Writables.strings())
 *          .count()
 *          .map(wordCountPair -> wordCountPair.first() + ": " + wordCountPair.second(), strings())
 *          .write(To.textFile("/path/to/output/file"));
 * pipeline.run();
 * }</pre>
 *
 */
public class Lambda {
    private static LCollectionFactory INSTANCE = new LCollectionFactoryImpl();

    public static <S> LCollection<S> wrap(PCollection<S> collection) { return INSTANCE.wrap(collection); }
    public static <K, V> LTable<K, V> wrap(PTable<K, V> collection) { return INSTANCE.wrap(collection); }
    public static <K, V> LGroupedTable<K, V> wrap(PGroupedTable<K, V> collection) { return INSTANCE.wrap(collection); }
}
