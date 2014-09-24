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

import com.google.common.collect.Lists;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * An {@code Iterator<T>} that combines a delegate {@code Iterator<S>} and a {@code DoFn<S, T>}, generating
 * data by passing the contents of the iterator through the function. Note that the input {@code DoFn} should
 * have both its {@code setContext} and {@code initialize} functions called <b>before</b> it is passed to
 * the constructor.
 *
 * @param <S> The type of the delegate iterator
 * @param <T> The returned type
 */
public class DoFnIterator<S, T> implements Iterator<T> {

  private final Iterator<S> iter;
  private final DoFn<S, T> fn;
  private CacheEmitter<T> cache;
  private boolean cleanup;

  public DoFnIterator(Iterator<S> iter, DoFn<S, T> fn) {
    this.iter = iter;
    this.fn = fn;
    this.cache = new CacheEmitter<T>();
    this.cleanup = false;
  }

  @Override
  public boolean hasNext() {
    while (cache.isEmpty() && iter.hasNext()) {
      fn.process(iter.next(), cache);
    }
    if (cache.isEmpty() && !cleanup) {
      fn.cleanup(cache);
      cleanup = true;
    }
    return !cache.isEmpty();
  }

  @Override
  public T next() {
    return cache.poll();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  private static class CacheEmitter<T> implements Emitter<T> {

    private final LinkedList<T> cache;

    private CacheEmitter() {
      this.cache = Lists.newLinkedList();
    }

    public synchronized boolean isEmpty() {
      return cache.isEmpty();
    }

    public synchronized T poll() {
      return cache.poll();
    }

    @Override
    public synchronized void emit(T emitted) {
      cache.add(emitted);
    }

    @Override
    public void flush() {
      // No-op
    }
  }
}
