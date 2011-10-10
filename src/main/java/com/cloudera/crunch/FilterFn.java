/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.crunch;

import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * A {@link DoFn} for the common case of filtering the members of
 * a {@link PCollection} based on a boolean condition.
 *
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
  
  public static <S> FilterFn<S> and(FilterFn<S>...fns) {
    return new AndFn<S>(fns);
  }
  
  public static class AndFn<S> extends FilterFn<S> {
    private final List<FilterFn<S>> fns;
    
    public AndFn(FilterFn<S>... fns) {
      this.fns = ImmutableList.<FilterFn<S>>copyOf(fns);
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
  
  public static <S> FilterFn<S> or(FilterFn<S>...fns) {
    return new OrFn<S>(fns);
  }
  
  public static class OrFn<S> extends FilterFn<S> {
    private final List<FilterFn<S>> fns;
    
    public OrFn(FilterFn<S>... fns) {
      this.fns = ImmutableList.<FilterFn<S>>copyOf(fns);
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
  
  public static <S> FilterFn<S> not(FilterFn<S> fn) {
    return new NotFn<S>(fn);
  }
  
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
