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
package org.apache.crunch.fn;

import com.google.common.collect.ImmutableList;
import org.apache.crunch.FilterFn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import java.util.List;


/**
 * A collection of pre-defined {@link FilterFn} implementations.
 */
public final class FilterFns {
  // Note: We delegate to the deprecated implementation classes in FilterFn. When their
  //       time is up, we just move them here.

  private FilterFns() {
    // utility class, not for instantiation
  }

  /**
   * Accept an entry if all of the given filters accept it, using short-circuit evaluation.
   * @param fn1 The first functions to delegate to
   * @param fn2 The second functions to delegate to
   * @return The composed filter function
   */
  public static <S> FilterFn<S> and(FilterFn<S> fn1, FilterFn<S> fn2) {
    return new AndFn<S>(fn1, fn2);
  }

  /**
   * Accept an entry if all of the given filters accept it, using short-circuit evaluation.
   * @param fns The functions to delegate to (in the given order)
   * @return The composed filter function
   */
  public static <S> FilterFn<S> and(FilterFn<S>... fns) {
    return new AndFn<S>(fns);
  }

  /**
   * Accept an entry if at least one of the given filters accept it, using short-circuit evaluation.
   * @param fn1 The first functions to delegate to
   * @param fn2 The second functions to delegate to
   * @return The composed filter function
   */
  public static <S> FilterFn<S> or(FilterFn<S> fn1, FilterFn<S> fn2) {
    return new OrFn<S>(fn1, fn2);
  }

  /**
   * Accept an entry if at least one of the given filters accept it, using short-circuit evaluation.
   * @param fns The functions to delegate to (in the given order)
   * @return The composed filter function
   */
  public static <S> FilterFn<S> or(FilterFn<S>... fns) {
    return new OrFn<S>(fns);
  }

  /**
   * Accept an entry if the given filter <em>does not</em> accept it.
   * @param fn The function to delegate to
   * @return The composed filter function
   */
  public static <S> FilterFn<S> not(FilterFn<S> fn) {
    return new NotFn<S>(fn);
  }

  /**
   * Accept everything.
   * @return A filter function that accepts everything.
   */
  public static <S> FilterFn<S> ACCEPT_ALL() {
    return new AcceptAllFn<S>();
  }

  /**
   * Reject everything.
   * @return A filter function that rejects everything.
   */
  public static <S> FilterFn<S> REJECT_ALL() {
    return not(new AcceptAllFn<S>());
  }

  private static class AndFn<S> extends FilterFn<S> {

    private final List<FilterFn<S>> fns;

    public AndFn(FilterFn<S>... fns) {
      this.fns = ImmutableList.<FilterFn<S>> copyOf(fns);
    }

    @Override
    public void configure(Configuration conf) {
      for (FilterFn<S> fn : fns) {
        fn.configure(conf);
      }
    }

    @Override
    public void setContext(TaskInputOutputContext<?, ?, ?, ?> context) {
      for (FilterFn<S> fn : fns) {
        fn.setContext(context);
      }
    }

    @Override
    public void initialize() {
      for (FilterFn<S> fn : fns) {
        fn.initialize();
      }
    }

    @Override
    public void cleanup() {
      for (FilterFn<S> fn : fns) {
        fn.cleanup();
      }
    }

    @Override
    public boolean accept(S input) {
      for (FilterFn<S> fn : fns) {
        if (!fn.accept(input)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public float scaleFactor() {
      float scaleFactor = 1.0f;
      for (FilterFn<S> fn : fns) {
        scaleFactor *= fn.scaleFactor();
      }
      return scaleFactor;
    }
  }

  private static class OrFn<S> extends FilterFn<S> {

    private final List<FilterFn<S>> fns;

    public OrFn(FilterFn<S>... fns) {
      this.fns = ImmutableList.<FilterFn<S>> copyOf(fns);
    }

    @Override
    public void configure(Configuration conf) {
      for (FilterFn<S> fn : fns) {
        fn.configure(conf);
      }
    }

    @Override
    public void setContext(TaskInputOutputContext<?, ?, ?, ?> context) {
      for (FilterFn<S> fn : fns) {
        fn.setContext(context);
      }
    }

    @Override
    public void initialize() {
      for (FilterFn<S> fn : fns) {
        fn.initialize();
      }
    }

    @Override
    public void cleanup() {
      for (FilterFn<S> fn : fns) {
        fn.cleanup();
      }
    }

    @Override
    public boolean accept(S input) {
      for (FilterFn<S> fn : fns) {
        if (fn.accept(input)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public float scaleFactor() {
      float scaleFactor = 0.0f;
      for (FilterFn<S> fn : fns) {
        scaleFactor += fn.scaleFactor();
      }
      return Math.min(1.0f, scaleFactor);
    }
  }

  private static class NotFn<S> extends FilterFn<S> {

    private final FilterFn<S> base;

    public NotFn(FilterFn<S> base) {
      this.base = base;
    }

    @Override
    public void configure(Configuration conf) {
      base.configure(conf);
    }

    @Override
    public void setContext(TaskInputOutputContext<?, ?, ?, ?> context) {
      base.setContext(context);
    }

    @Override
    public void initialize() {
      base.initialize();
    }

    @Override
    public void cleanup() {
      base.cleanup();
    }

    @Override
    public boolean accept(S input) {
      return !base.accept(input);
    }

    @Override
    public float scaleFactor() {
      return 1.0f - base.scaleFactor();
    }
  }

  private static class AcceptAllFn<S> extends FilterFn<S> {
    @Override
    public boolean accept(S input) {
      return true;
    }

    @Override
    public float scaleFactor() {
      return 1.0f;
    }
  }

}
