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
package org.apache.crunch;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;


/**
 * Aggregate a sequence of values into a possibly smaller sequence of the same type.
 *
 * <p>In most cases, an Aggregator will turn multiple values into a single value,
 * like creating a sum, finding the minimum or maximum, etc. In some cases
 * (ie. finding the top K elements), an implementation may return more than
 * one value. The {@link org.apache.crunch.fn.Aggregators} utility class contains
 * factory methods for creating all kinds of pre-defined Aggregators that should
 * cover the most common cases.</p>
 *
 * <p>Aggregator implementations should usually be <em>associative</em> and
 * <em>commutative</em>, which makes their results deterministic. If your aggregation
 * function isn't commutative, you can still use secondary sort to that effect.</p>
 *
 * <p>The lifecycle of an {@link Aggregator} always begins with you instantiating
 * it and passing it to Crunch. When running your {@link Pipeline}, Crunch serializes
 * the instance and deserializes it wherever it is needed on the cluster. This is how
 * Crunch uses a deserialized instance:<p>
 *
 * <ol>
 *   <li>call {@link #initialize(Configuration)} once</li>
 *   <li>call {@link #reset()}
 *   <li>call {@link #update(Object)} multiple times until all values of a sequence
 *       have been aggregated</li>
 *   <li>call {@link #results()} to retrieve the aggregated result</li>
 *   <li>go back to step 2 until all sequences have been aggregated</li>
 * </ol>
 *
 * @param <T> The value types to aggregate
 */
public interface Aggregator<T> extends Serializable {

  /**
   * Perform any setup of this instance that is required prior to processing
   * inputs.
   *
   * @param conf Hadoop configuration
   */
  void initialize(Configuration conf);

  /**
   * Clears the internal state of this Aggregator and prepares it for the
   * values associated with the next key.
   *
   * Depending on what you aggregate, this typically means setting a variable
   * to zero or clearing a list. Failing to do this will yield wrong results!
   */
  void reset();

  /**
   * Incorporate the given value into the aggregate state maintained by this
   * instance.
   *
   * @param value The value to add to the aggregated state
   */
  void update(T value);

  /**
   * Returns the current aggregated state of this instance.
   */
  Iterable<T> results();
}
