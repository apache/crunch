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

import java.util.List;

import org.apache.crunch.fn.FilterFns;

import com.google.common.collect.ImmutableList;

/**
 * A {@link DoFn} for the common case of filtering the members of a
 * {@link PCollection} based on a boolean condition.
 */
public abstract class FilterFn<T> extends DoFn<T, T> {

  /**
   * If true, emit the given record.
   */
  public abstract boolean accept(T input);

  @Override
  public void process(T input, Emitter<T> emitter) {
    if (accept(input)) {
      emitter.emit(input);
    }
  }

  @Override
  public float scaleFactor() {
    return 0.5f;
  }

  /**
   * @deprecated Use {@link FilterFns#and(FilterFn...)}
   */
  public static <S> FilterFn<S> and(FilterFn<S>... fns) {
    return new AndFn<S>(fns);
  }

  /**
   * @deprecated Use {@link FilterFns#and(FilterFn...)}
   */
  public static class AndFn<S> extends FilterFn<S> {

    private final List<FilterFn<S>> fns;

    public AndFn(FilterFn<S>... fns) {
      this.fns = ImmutableList.<FilterFn<S>> copyOf(fns);
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

  /**
   * @deprecated Use {@link FilterFns#or(FilterFn...)}
   */
  public static <S> FilterFn<S> or(FilterFn<S>... fns) {
    return new OrFn<S>(fns);
  }

  /**
   * @deprecated Use {@link FilterFns#or(FilterFn...)}
   */
  public static class OrFn<S> extends FilterFn<S> {

    private final List<FilterFn<S>> fns;

    public OrFn(FilterFn<S>... fns) {
      this.fns = ImmutableList.<FilterFn<S>> copyOf(fns);
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

  /**
   * @deprecated Use {@link FilterFns#not(FilterFn)}
   */
  public static <S> FilterFn<S> not(FilterFn<S> fn) {
    return new NotFn<S>(fn);
  }

  /**
   * @deprecated Use {@link FilterFns#not(FilterFn)}
   */
  public static class NotFn<S> extends FilterFn<S> {

    private final FilterFn<S> base;

    public NotFn(FilterFn<S> base) {
      this.base = base;
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
}
