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

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;

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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.base.Preconditions;

/**
 * Static functions for working with legacy Mappers and Reducers that live under the org.apache.hadoop.mapreduce.*
 * package as part of Crunch pipelines.
 */
public class Mapreduce {
  
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
      DoFn<Pair<K1, V1>, Pair<K2, V2>> {
    private final Class<? extends Mapper<K1, V1, K2, V2>> mapperClass;
    private transient Mapper<K1, V1, K2, V2> instance;
    private transient Mapper.Context context;
    private transient CtxtMethodHandler handler;
    private transient Method setupMethod;
    private transient Method mapMethod;
    private transient Method cleanupMethod;
    
    public MapperFn(Class<? extends Mapper<K1, V1, K2, V2>> mapperClass) {
      this.mapperClass = Preconditions.checkNotNull(mapperClass);
    }
    
    @Override
    public void initialize() {
      if (instance == null) {
        this.instance = ReflectionUtils.newInstance(mapperClass, getConfiguration());
        try {
          for (Method m : mapperClass.getDeclaredMethods()) {
            if ("setup".equals(m.getName())) {
              this.setupMethod = m;
              this.setupMethod.setAccessible(true);
            } else if ("cleanup".equals(m.getName())) {
              this.cleanupMethod = m;
              this.cleanupMethod.setAccessible(true);
            } else if ("map".equals(m.getName())) {
              this.mapMethod = m;
              this.mapMethod.setAccessible(true);
            }
          }
          
          if (mapMethod == null) {
            throw new CrunchRuntimeException("No map method for class: " + mapperClass);
          }
          
          ProxyFactory proxyFactory = new ProxyFactory();
          proxyFactory.setSuperclass(Mapper.Context.class);
          proxyFactory.setFilter(CtxtMethodHandler.FILTER);
          Class[] paramTypes = new Class[] { Mapper.class };
          Object[] args = new Object[] { instance };
          if (!Modifier.isAbstract(Mapper.Context.class.getModifiers())) {
            paramTypes = new Class[] { Mapper.class,
                Configuration.class, TaskAttemptID.class,
                RecordReader.class, RecordWriter.class,
                OutputCommitter.class,
                Class.forName("org.apache.hadoop.mapreduce.StatusReporter"),
                InputSplit.class
            };
            args = new Object[] { instance, getConfiguration(), getTaskAttemptID(),
                null, null, NO_OP_OUTPUT_COMMITTER, null, null
            };
          }
          this.handler = new CtxtMethodHandler(this.getContext());
          this.context = (Mapper.Context) proxyFactory.create(paramTypes, args, handler);
        } catch (Exception e) {
          throw new CrunchRuntimeException(e);
        }
      }
      if (setupMethod != null) {
        try {
          setupMethod.invoke(instance, context);
        } catch (Exception e) {
          throw new CrunchRuntimeException(e);
        }
      }
    }
    
    @Override
    public void process(Pair<K1, V1> input, Emitter<Pair<K2, V2>> emitter) {
      handler.set(emitter);
      try {
        mapMethod.invoke(instance, input.first(), input.second(), context);
      } catch (Exception e) {
        throw new CrunchRuntimeException(e);
      }
    }
    
    @Override
    public void cleanup(Emitter<Pair<K2, V2>> emitter) {
      if (cleanupMethod != null) {
        handler.set(emitter);
        try {
          cleanupMethod.invoke(instance, context);
        } catch (Exception e) {
          throw new CrunchRuntimeException(e);
        }
      }
    }
  }
  
  private static class ReducerFn<K1, V1, K2 extends Writable, V2 extends Writable> extends
      DoFn<Pair<K1, Iterable<V1>>, Pair<K2, V2>> {
    private final Class<? extends Reducer<K1, V1, K2, V2>> reducerClass;
    private transient Reducer<K1, V1, K2, V2> instance;
    private transient CtxtMethodHandler handler;
    private transient Reducer.Context context;
    private transient Method setupMethod;
    private transient Method reduceMethod;
    private transient Method cleanupMethod;

    public ReducerFn(Class<? extends Reducer<K1, V1, K2, V2>> reducerClass) {
      this.reducerClass = Preconditions.checkNotNull(reducerClass);
    }

    @Override
    public void initialize() {
      if (instance == null) {
        this.instance = ReflectionUtils.newInstance(reducerClass, getConfiguration());
        try {
          for (Method m : reducerClass.getDeclaredMethods()) {
            if ("setup".equals(m.getName())) {
              this.setupMethod = m;
              this.setupMethod.setAccessible(true);
            } else if ("cleanup".equals(m.getName())) {
              this.cleanupMethod = m;
              this.cleanupMethod.setAccessible(true);
            } else if ("reduce".equals(m.getName())) {
              this.reduceMethod = m;
              this.reduceMethod.setAccessible(true);
            }
          }
          
          if (reduceMethod == null) {
            throw new CrunchRuntimeException("No reduce method for class: " + reducerClass);
          }

          ProxyFactory proxyFactory = new ProxyFactory();
          proxyFactory.setSuperclass(Reducer.Context.class);
          proxyFactory.setFilter(CtxtMethodHandler.FILTER);
          Class[] paramTypes = new Class[] { Reducer.class };
          Object[] args = new Object[] { instance };
          if (!Modifier.isAbstract(Reducer.Context.class.getModifiers())) {
            Class rkvi = Class.forName("org.apache.hadoop.mapred.RawKeyValueIterator"); 
            Object rawKeyValueIterator = Proxy.newProxyInstance(rkvi.getClassLoader(),
                new Class[] { rkvi }, new InvocationHandler() {
                  @Override
                  public Object invoke(Object obj, Method m, Object[] args) throws Throwable {
                    if ("next".equals(m.getName())) {
                      return true;
                    }
                    return null;
                  }
            });
            paramTypes = new Class[] { Reducer.class,
                Configuration.class, TaskAttemptID.class,
                rkvi,
                Counter.class, Counter.class,
                RecordWriter.class,
                OutputCommitter.class,
                Class.forName("org.apache.hadoop.mapreduce.StatusReporter"),
                RawComparator.class,
                Class.class, Class.class
            };
            args = new Object[] { instance, getConfiguration(), getTaskAttemptID(),
                rawKeyValueIterator, null, null, null,
                NO_OP_OUTPUT_COMMITTER, null, null,
                NullWritable.class, NullWritable.class
            };
          }
          this.handler = new CtxtMethodHandler(this.getContext());
          this.context = (Reducer.Context) proxyFactory.create(paramTypes, args, handler);
        } catch (Exception e) {
          throw new CrunchRuntimeException(e);
        }
      }
      
      if (setupMethod != null) {
        try {
          setupMethod.invoke(instance, context);
        } catch (Exception e) {
          throw new CrunchRuntimeException(e);
        }
      }
    }

    @Override
    public void process(Pair<K1, Iterable<V1>> input, Emitter<Pair<K2, V2>> emitter) {
      handler.set(emitter);
      try {
        reduceMethod.invoke(instance, input.first(), input.second(), context);
      } catch (Exception e) {
        throw new CrunchRuntimeException(e);
      }
    }

    @Override
    public void cleanup(Emitter<Pair<K2, V2>> emitter) {
      if (cleanupMethod != null) {
        handler.set(emitter);
        try {
          cleanupMethod.invoke(instance, context);
        } catch (Exception e) {
          throw new CrunchRuntimeException(e);
        }
      }
    }
  }
  
  private static class CtxtMethodHandler implements MethodHandler {
    public static final MethodFilter FILTER = new MethodFilter() {
      @Override
      public boolean isHandled(Method m) {
        return true;
      }
    };
      
    private final TaskInputOutputContext ctxt;
    private Emitter emitter;
    
    public CtxtMethodHandler(TaskInputOutputContext ctxt) {
      this.ctxt = ctxt;
    }
    
    public void set(Emitter emitter) {
      this.emitter = emitter;
    }
    
    @Override
    public Object invoke(Object instance, Method m, Method arg2, Object[] args) throws Throwable {
      String name = m.getName();
      if ("write".equals(name)) {
        emitter.emit(Pair.of(args[0], args[1]));
        return null;
      } else {
        return m.invoke(ctxt, args);
      }
    }
  }
  
  private static final OutputCommitter NO_OP_OUTPUT_COMMITTER = new OutputCommitter() {
    @Override
    public void abortTask(TaskAttemptContext arg0) throws IOException {
    }

    @Override
    public void commitTask(TaskAttemptContext arg0) throws IOException {
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext arg0) throws IOException {
      return false;
    }

    @Override
    public void setupJob(JobContext arg0) throws IOException {
    }

    @Override
    public void setupTask(TaskAttemptContext arg0) throws IOException {
    }
  };
  
}
