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

import java.util.Iterator;
import java.util.List;

import org.apache.crunch.Pair;
import org.apache.crunch.Tuple3;
import org.apache.crunch.Tuple4;
import org.apache.crunch.TupleN;

import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;

/**
 * Utilities for working with subclasses of the {@code Tuple} interface.
 * 
 */
public class Tuples {

  private static abstract class TuplifyIterator<T> extends UnmodifiableIterator<T> {
    protected List<Iterator<?>> iterators;

    public TuplifyIterator(Iterator<?>... iterators) {
      this.iterators = Lists.newArrayList(iterators);
    }

    @Override
    public boolean hasNext() {
      for (Iterator<?> iter : iterators) {
        if (!iter.hasNext()) {
          return false;
        }
      }
      return true;
    }

    protected Object next(int index) {
      return iterators.get(index).next();
    }
  }

  public static class PairIterable<S, T> implements Iterable<Pair<S, T>> {
    private final Iterable<S> first;
    private final Iterable<T> second;

    public PairIterable(Iterable<S> first, Iterable<T> second) {
      this.first = first;
      this.second = second;
    }

    @Override
    public Iterator<Pair<S, T>> iterator() {
      return new TuplifyIterator<Pair<S, T>>(first.iterator(), second.iterator()) {
        @Override
        public Pair<S, T> next() {
          return Pair.of((S) next(0), (T) next(1));
        }
      };
    }
  }

  public static class TripIterable<A, B, C> implements Iterable<Tuple3<A, B, C>> {
    private final Iterable<A> first;
    private final Iterable<B> second;
    private final Iterable<C> third;

    public TripIterable(Iterable<A> first, Iterable<B> second, Iterable<C> third) {
      this.first = first;
      this.second = second;
      this.third = third;
    }

    @Override
    public Iterator<Tuple3<A, B, C>> iterator() {
      return new TuplifyIterator<Tuple3<A, B, C>>(first.iterator(), second.iterator(), third.iterator()) {
        @Override
        public Tuple3<A, B, C> next() {
          return new Tuple3<A, B, C>((A) next(0), (B) next(1), (C) next(2));
        }
      };
    }
  }

  public static class QuadIterable<A, B, C, D> implements Iterable<Tuple4<A, B, C, D>> {
    private final Iterable<A> first;
    private final Iterable<B> second;
    private final Iterable<C> third;
    private final Iterable<D> fourth;

    public QuadIterable(Iterable<A> first, Iterable<B> second, Iterable<C> third, Iterable<D> fourth) {
      this.first = first;
      this.second = second;
      this.third = third;
      this.fourth = fourth;
    }

    @Override
    public Iterator<Tuple4<A, B, C, D>> iterator() {
      return new TuplifyIterator<Tuple4<A, B, C, D>>(first.iterator(), second.iterator(), third.iterator(),
          fourth.iterator()) {
        @Override
        public Tuple4<A, B, C, D> next() {
          return new Tuple4<A, B, C, D>((A) next(0), (B) next(1), (C) next(2), (D) next(3));
        }
      };
    }
  }

  public static class TupleNIterable implements Iterable<TupleN> {
    private final Iterator<?>[] iters;

    public TupleNIterable(Iterable<?>... iterables) {
      this.iters = new Iterator[iterables.length];
      for (int i = 0; i < iters.length; i++) {
        iters[i] = iterables[i].iterator();
      }
    }

    @Override
    public Iterator<TupleN> iterator() {
      return new TuplifyIterator<TupleN>(iters) {
        @Override
        public TupleN next() {
          Object[] values = new Object[iters.length];
          for (int i = 0; i < values.length; i++) {
            values[i] = next(i);
          }
          return new TupleN(values);
        }
      };
    }
  }
}
