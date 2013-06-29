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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Set;

import javassist.util.proxy.MethodFilter;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;

import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

/**
 * Static functions for working with legacy Mappers and Reducers that live under the org.apache.hadoop.mapred.*
 * package as part of Crunch pipelines.
 */
public class Mapred {

  public static <K1, V1, K2 extends Writable, V2 extends Writable> PTable<K2, V2> map(
      PTable<K1, V1> input,
      Class<? extends Mapper<K1, V1, K2, V2>> mapperClass,
      Class<K2> keyClass, Class<V2> valueClass) {
    return input.parallelDo(new MapperFn<K1, V1, K2, V2>(mapperClass), tableOf(keyClass, valueClass));
  }
  
  public static <K1, V1, K2 extends Writable, V2 extends Writable> PTable<K2, V2> reduce(
      PGroupedTable<K1, V1> input,
      Class<? extends Reducer<K1, V1, K2, V2>> reducerClass,
          Class<K2> keyClass, Class<V2> valueClass) {
    return input.parallelDo(new ReducerFn<K1, V1, K2, V2>(reducerClass), tableOf(keyClass, valueClass));
  }
  
  private static <K extends Writable, V extends Writable> PTableType<K, V> tableOf(
      Class<K> keyClass, Class<V> valueClass) {
    return Writables.tableOf(Writables.writables(keyClass), Writables.writables(valueClass));
  }
  
  private static class MapperFn<K1, V1, K2 extends Writable, V2 extends Writable> extends
      DoFn<Pair<K1, V1>, Pair<K2, V2>> implements Reporter {
    private final Class<? extends Mapper<K1, V1, K2, V2>> mapperClass;
    private transient Mapper<K1, V1, K2, V2> instance;
    private transient OutputCollectorImpl<K2, V2> outputCollector;
    
    public MapperFn(Class<? extends Mapper<K1, V1, K2, V2>> mapperClass) {
      this.mapperClass = Preconditions.checkNotNull(mapperClass);
    }
    
    @Override
    public void initialize() {
      if (instance == null) {
        this.instance = ReflectionUtils.newInstance(mapperClass, getConfiguration());
      }
      instance.configure(new JobConf(getConfiguration()));
      outputCollector = new OutputCollectorImpl<K2, V2>();
    }
    
    @Override
    public void process(Pair<K1, V1> input, Emitter<Pair<K2, V2>> emitter) {
      outputCollector.set(emitter);
      try {
        instance.map(input.first(), input.second(), outputCollector, this);
      } catch (IOException e) {
        throw new CrunchRuntimeException(e);
      }
    }
    
    @Override
    public void cleanup(Emitter<Pair<K2, V2>> emitter) {
      try {
        instance.close();
      } catch (IOException e) {
        throw new CrunchRuntimeException("Error closing mapper = " + mapperClass, e);
      }
    }

    @Override
    public void progress() {
      super.progress();
    }
    
    @Override
    public void setStatus(String status) {
      super.setStatus(status);
    }
    
    public Counters.Counter getCounter(Enum<?> counter) {
      return proxyCounter(super.getCounter(counter));
    }
    
    public Counters.Counter getCounter(String group, String name) {
      return proxyCounter(super.getCounter(group, name));
    }
    
    @Override
    public InputSplit getInputSplit() throws UnsupportedOperationException {
      return null;
    }

    @Override
    public void incrCounter(Enum<?> counter, long by) {
      super.increment(counter, by);
    }

    @Override
    public void incrCounter(String group, String name, long by) {
      super.increment(group, name, by);
    }
    
    public float getProgress() {
      return 0.5f;
    }
  }
  

  private static class ReducerFn<K1, V1, K2 extends Writable, V2 extends Writable> extends
      DoFn<Pair<K1, Iterable<V1>>, Pair<K2, V2>> implements Reporter {
    private final Class<? extends Reducer<K1, V1, K2, V2>> reducerClass;
    private transient Reducer<K1, V1, K2, V2> instance;
    private transient OutputCollectorImpl<K2, V2> outputCollector;

    public ReducerFn(Class<? extends Reducer<K1, V1, K2, V2>> reducerClass) {
      this.reducerClass = Preconditions.checkNotNull(reducerClass);
    }

    @Override
    public void initialize() {
      if (instance == null) {
        this.instance = ReflectionUtils.newInstance(reducerClass, getConfiguration());
      }
      instance.configure(new JobConf(getConfiguration()));
      outputCollector = new OutputCollectorImpl<K2, V2>();
    }

    @Override
    public void process(Pair<K1, Iterable<V1>> input, Emitter<Pair<K2, V2>> emitter) {
      outputCollector.set(emitter);
      try {
        instance.reduce(input.first(), input.second().iterator(), outputCollector, this);
      } catch (IOException e) {
        throw new CrunchRuntimeException(e);
      }
    }

    @Override
    public void cleanup(Emitter<Pair<K2, V2>> emitter) {
      try {
        instance.close();
      } catch (IOException e) {
        throw new CrunchRuntimeException("Error closing mapper = " + reducerClass, e);
      }
    }

    @Override
    public void progress() {
      super.progress();
    }

    @Override
    public void setStatus(String status) {
      super.setStatus(status);
    }

    public Counters.Counter getCounter(Enum<?> counter) {
      return proxyCounter(super.getCounter(counter));
    }

    public Counters.Counter getCounter(String group, String name) {
      return proxyCounter(super.getCounter(group, name));
    }

    @Override
    public InputSplit getInputSplit() throws UnsupportedOperationException {
      return null;
    }

    @Override
    public void incrCounter(Enum<?> counter, long by) {
      super.increment(counter, by);
    }

    @Override
    public void incrCounter(String group, String name, long by) {
      super.increment(group, name, by);
    }
    
    public float getProgress() {
      return 0.5f;
    }
  }

  private static class OutputCollectorImpl<K, V> implements OutputCollector<K, V> {
    private Emitter<Pair<K, V>> emitter;
    
    public OutputCollectorImpl() { }
    
    public void set(Emitter<Pair<K, V>> emitter) {
      this.emitter = emitter;
    }
    
    @Override
    public void collect(K k, V v) throws IOException {
      emitter.emit(Pair.of(k, v));
    }
  }
  
  private static Counters.Counter proxyCounter(Counter c) {
    ProxyFactory proxyFactory = new ProxyFactory();
    proxyFactory.setSuperclass(Counters.Counter.class);
    proxyFactory.setFilter(CCMethodHandler.FILTER);
    CCMethodHandler handler = new CCMethodHandler(c);
    try {
      return (Counters.Counter) proxyFactory.create(new Class[0], new Object[0], handler);
    } catch (Exception e) {
      throw new CrunchRuntimeException(e);
    }
  }
  
  private static class CCMethodHandler implements MethodHandler {
    private static final Set<String> HANDLED = ImmutableSet.of("increment",
        "getCounter", "getValue", "getName", "getDisplayName", "setValue",
        "getUnderlyingCounter", "readFields", "write");
    public static final MethodFilter FILTER = new MethodFilter() {
      @Override
      public boolean isHandled(Method m) {
        return HANDLED.contains(m.getName());
      }
    };
    
    private final Counter c;
    
    public CCMethodHandler(Counter c) {
      this.c = c;
    }
    
    @Override
    public Object invoke(Object obj, Method m, Method m2, Object[] args) throws Throwable {
      String name = m.getName();
      if ("increment".equals(name)) {
        c.increment((Long) args[0]);
        return null;
      } else if ("getCounter".equals(name) || "getValue".equals(name)) {
        return c.getValue();
      } else if ("setValue".equals(name)) {
        c.setValue((Long) args[0]);
        return null;
      } else if ("getDisplayName".equals(name)) {
        return c.getDisplayName();
      } else if ("getName".equals(name)) {
        return c.getName();
      } else if ("getUnderlyingCounter".equals(name)) {
        return c;
      } else if ("readFields".equals(name)) {
        c.readFields((DataInput) args[0]);
        return null;
      } else if ("write".equals(name)) {
        c.write((DataOutput) args[0]);
        return null;
      }
      throw new IllegalStateException("Unhandled Counters.Counter method = " + name);
    }
  }
}
