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

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.FilterFn;
import org.apache.crunch.IDoFn;
import org.apache.crunch.IFilterFn;
import org.apache.crunch.IFlatMapFn;
import org.apache.crunch.IMapFn;
import org.apache.crunch.MapFn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public class IFnHelpers {

  public static <S, T> DoFn<S, T> wrap(final org.apache.crunch.IDoFn<S, T> fn) {
    return new IDoFnWrapper(fn);
  }

  public static <S, T> DoFn<S, T> wrapFlatMap(final IFlatMapFn<S, T> fn) {
    return new DoFn<S, T>() {
      @Override
      public void process(S input, Emitter<T> emitter) {
        for (T t : fn.process(input)) {
          emitter.emit(t);
        }
      }
    };
  }

  public static <S, T> MapFn<S, T> wrapMap(final IMapFn<S, T> fn) {
    return new MapFn<S, T>() {
      @Override
      public T map(S input) {
        return fn.map(input);
      }
    };
  }

  public static <S> FilterFn<S> wrapFilter(final IFilterFn<S> fn) {
    return new FilterFn<S>() {
      @Override
      public boolean accept(S input) {
        return fn.accept(input);
      }
    };
  }

  static class IDoFnWrapper<S, T> extends DoFn<S, T> {

    private final org.apache.crunch.IDoFn<S, T> fn;
    private transient ContextImpl<S, T> ctxt;

    public IDoFnWrapper(org.apache.crunch.IDoFn<S, T> fn) {
      this.fn = fn;
    }

    @Override
    public void initialize() {
      super.initialize();
      if (getContext() == null) {
        this.ctxt = new ContextImpl<S, T>(getConfiguration());
      } else {
        this.ctxt = new ContextImpl<S, T>(getContext());
      }
    }

    @Override
    public void process(S input, Emitter<T> emitter) {
      fn.process(ctxt.update(input, emitter));
    }
  }

  static class ContextImpl<S, T> implements IDoFn.Context<S, T> {
    private S element;
    private Emitter<T> emitter;
    private TaskInputOutputContext context;
    private Configuration conf;

    public ContextImpl(TaskInputOutputContext context) {
      this.context = context;
      this.conf = context.getConfiguration();
    }

    public ContextImpl(Configuration conf) {
      this.context = null;
      this.conf = conf;
    }

    public ContextImpl update(S element, Emitter<T> emitter) {
      this.element = element;
      this.emitter = emitter;
      return this;
    }

    public S element() {
      return element;
    }

    public void emit(T t) {
      emitter.emit(t);
    }

    public TaskInputOutputContext getContext() {
      return context;
    }

    public Configuration getConfiguration() {
      return conf;
    }

    public void increment(String groupName, String counterName) {
      increment(groupName, counterName, 1);
    }

    public void increment(String groupName, String counterName, long value) {
      if (context != null) {
        context.getCounter(groupName, counterName).increment(value);
      }
    }

    public void increment(Enum<?> counterName) {
      increment(counterName, 1);
    }

    public void increment(Enum<?> counterName, long value) {
      if (context != null) {
        context.getCounter(counterName).increment(value);
      }
    }
  }
}
