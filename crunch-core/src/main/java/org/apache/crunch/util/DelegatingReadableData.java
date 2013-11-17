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
package org.apache.crunch.util;

import org.apache.crunch.DoFn;
import org.apache.crunch.ReadableData;
import org.apache.crunch.SourceTarget;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

/**
 * Implements the {@code ReadableData<T>} interface by delegating to an {@code ReadableData<S>} instance
 * and passing its contents through a {@code DoFn<S, T>}.
 */
public class DelegatingReadableData<S, T> implements ReadableData<T> {

  private final ReadableData<S> delegate;
  private final DoFn<S, T> fn;

  public DelegatingReadableData(ReadableData<S> delegate, DoFn<S, T> fn) {
    this.delegate = delegate;
    this.fn = fn;
  }

  @Override
  public Set<SourceTarget<?>> getSourceTargets() {
    return delegate.getSourceTargets();
  }

  @Override
  public void configure(Configuration conf) {
    delegate.configure(conf);
    fn.configure(conf);
  }

  @Override
  public Iterable<T> read(TaskInputOutputContext<?, ?, ?, ?> context) throws IOException {
    fn.setContext(context);
    fn.initialize();
    final Iterable<S> delegateIterable = delegate.read(context);
    return new Iterable<T>() {
      @Override
      public Iterator<T> iterator() {
        return new DoFnIterator<S, T>(delegateIterable.iterator(), fn);
      }
    };
  }
}
