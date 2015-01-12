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
package org.apache.crunch.lib;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;

import java.io.Serializable;

public class DoFns {
  /**
   * "Reduce" DoFn wrapper which detaches the values in the iterable, preventing the unexpected behaviour related to
   * object reuse often observed when using Avro. Wrap your DoFn in a detach(...) and pass in a PType for the Iterable
   * value, and then you'll be handed an Iterable of real distinct objects, instead of the same object being handed to
   * you multiple times with different data.
   *
   * You should use this when you have a parallelDo after a groupBy, and you'd like to capture the objects arriving in
   * the Iterable part of the incoming Pair and pass it through to the output (for example if you want to create an
   * array of outputs from the values to be output as one record).
   *
   * The will incur a performance hit, as it means that every object read from the Iterable will allocate a new Java
   * object for the record and objects for all its non-primitive fields too. If you are rolling up records into a
   * collection then this will be necessary anyway, but if you are only outputting derived data this may impact the
   * speed and memory usage of your job unnecessarily.
   *
   * @param reduceFn Underlying DoFn to wrap
   * @param valueType PType of the object contained within the Iterable
   * @param <K> Reduce key
   * @param <V> Iterable value
   * @param <T> Output type of DoFn
   * @return DoFn which will detach values for you
   */
  public static <K, V, T> DoFn<Pair<K, Iterable<V>>, T> detach(final DoFn<Pair<K, Iterable<V>>, T> reduceFn, final PType<V> valueType) {
    return new DetachingDoFn<K, V, T>(reduceFn, valueType);
  }

  private static class DetachFunction<T> implements Function<T, T>, Serializable {
    private final PType<T> pType;

    public DetachFunction(PType<T> initializedPType) {
      this.pType = initializedPType;
    }

    @Override
    public T apply(T t) {
      return pType.getDetachedValue(t);
    }
  }

  private static class DetachingDoFn<K, V, T> extends DoFn<Pair<K, Iterable<V>>, T> {

    private final DoFn<Pair<K, Iterable<V>>, T> reduceFn;
    private final PType<V> valueType;

    public DetachingDoFn(DoFn<Pair<K, Iterable<V>>, T> reduceFn, PType<V> valueType) {
      this.reduceFn = reduceFn;
      this.valueType = valueType;
    }

    @Override
    public void configure(Configuration configuration) {
      super.configure(configuration);
      reduceFn.configure(configuration);
    }

    @Override
    public void initialize() {
      reduceFn.initialize();
      valueType.initialize(getConfiguration() == null ? new Configuration() : getConfiguration());
    }

    @Override
    public void process(Pair<K, Iterable<V>> input, Emitter<T> emitter) {
      reduceFn.process(Pair.of(input.first(), detachIterable(input.second(), valueType)), emitter);
    }

    public Iterable<V> detachIterable(Iterable<V> iterable, final PType<V> pType) {
      return Iterables.transform(iterable, new DetachFunction<V>(pType));
    }
  }
}
