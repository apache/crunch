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
package org.apache.crunch.impl.mem.collect;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Set;

import javassist.util.proxy.MethodFilter;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;

import org.apache.commons.lang.SerializationException;
import org.apache.commons.lang.SerializationUtils;
import org.apache.crunch.Aggregator;
import org.apache.crunch.CachingOptions;
import org.apache.crunch.DoFn;
import org.apache.crunch.FilterFn;
import org.apache.crunch.IFilterFn;
import org.apache.crunch.IFlatMapFn;
import org.apache.crunch.IDoFn;
import org.apache.crunch.IMapFn;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PObject;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.ParallelDoOptions;
import org.apache.crunch.Pipeline;
import org.apache.crunch.ReadableData;
import org.apache.crunch.PipelineCallable;
import org.apache.crunch.Target;
import org.apache.crunch.fn.ExtractKeyFn;
import org.apache.crunch.fn.IFnHelpers;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mem.emit.InMemoryEmitter;
import org.apache.crunch.lib.Aggregate;
import org.apache.crunch.materialize.pobject.CollectionPObject;
import org.apache.crunch.materialize.pobject.FirstElementPObject;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.util.ClassloaderFallbackObjectInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class MemCollection<S> implements PCollection<S> {

  private final Collection<S> collect;
  private final PType<S> ptype;
  private String name;

  public MemCollection(Iterable<S> collect) {
    this(collect, null, null);
  }

  public MemCollection(Iterable<S> collect, PType<S> ptype) {
    this(collect, ptype, null);
  }

  public MemCollection(Iterable<S> collect, PType<S> ptype, String name) {
    this.collect = ImmutableList.copyOf(collect);
    this.ptype = ptype;
    this.name = name;
  }

  @Override
  public Pipeline getPipeline() {
    return MemPipeline.getInstance();
  }

  @Override
  public PCollection<S> union(PCollection<S> other) {
    return union(new PCollection[] { other });
  }
  
  @Override
  public PCollection<S> union(PCollection<S>... collections) {
    return getPipeline().union(
        ImmutableList.<PCollection<S>>builder().add(this).add(collections).build());
  }

  private <S, T> DoFn<S, T> verifySerializable(String name, DoFn<S, T> doFn) {
    try {
      return (DoFn<S, T>) deserialize(SerializationUtils.serialize(doFn));
    } catch (SerializationException e) {
      throw new IllegalStateException(
          doFn.getClass().getSimpleName() + " named '" + name + "' cannot be serialized",
          e);
    }
  }

  // Use a custom deserialize implementation (not SerializationUtils) so we can fall back
  // to using the thread context classloader, which is needed when running Scrunch in
  // the Scala REPL
  private static Object deserialize(InputStream inputStream) {
    if (inputStream == null) {
      throw new IllegalArgumentException("The InputStream must not be null");
    }
    ObjectInputStream in = null;
    try {
      // stream closed in the finally
      in = new ClassloaderFallbackObjectInputStream(inputStream);
      return in.readObject();

    } catch (ClassNotFoundException ex) {
      throw new SerializationException(ex);
    } catch (IOException ex) {
      throw new SerializationException(ex);
    } finally {
      try {
        if (in != null) {
          in.close();
        }
      } catch (IOException ex) {
        // ignore close exception
      }
    }
  }

  private static Object deserialize(byte[] objectData) {
    if (objectData == null) {
      throw new IllegalArgumentException("The byte[] must not be null");
    }
    ByteArrayInputStream bais = new ByteArrayInputStream(objectData);
    return deserialize(bais);
  }

  @Override
  public <T> PCollection<T> parallelDo(DoFn<S, T> doFn, PType<T> type) {
    return parallelDo(null, doFn, type);
  }

  @Override
  public <T> PCollection<T> parallelDo(String name, DoFn<S, T> doFn, PType<T> type) {
    return parallelDo(name, doFn, type, ParallelDoOptions.builder().build());
  }
  
  @Override
  public <T> PCollection<T> parallelDo(String name, DoFn<S, T> doFn, PType<T> type,
      ParallelDoOptions options) {
    doFn = verifySerializable(name, doFn);
    InMemoryEmitter<T> emitter = new InMemoryEmitter<T>();
    Configuration conf = getPipeline().getConfiguration();
    doFn.configure(conf);
    doFn.setContext(getInMemoryContext(conf));
    doFn.initialize();
    for (S s : collect) {
      doFn.process(s, emitter);
    }
    doFn.cleanup(emitter);
    return new MemCollection<T>(emitter.getOutput(), type, name);
  }

  @Override
  public <K, V> PTable<K, V> parallelDo(DoFn<S, Pair<K, V>> doFn, PTableType<K, V> type) {
    return parallelDo(null, doFn, type);
  }

  @Override
  public <K, V> PTable<K, V> parallelDo(String name, DoFn<S, Pair<K, V>> doFn, PTableType<K, V> type) {
    return parallelDo(name, doFn, type, ParallelDoOptions.builder().build());
  }
  
  @Override
  public <K, V> PTable<K, V> parallelDo(String name, DoFn<S, Pair<K, V>> doFn, PTableType<K, V> type,
      ParallelDoOptions options) {
    InMemoryEmitter<Pair<K, V>> emitter = new InMemoryEmitter<Pair<K, V>>();
    Configuration conf = getPipeline().getConfiguration();
    doFn.configure(conf);
    doFn.setContext(getInMemoryContext(conf));
    doFn.initialize();
    for (S s : collect) {
      doFn.process(s, emitter);
    }
    doFn.cleanup(emitter);
    return new MemTable<K, V>(emitter.getOutput(), type, name);
  }

  @Override
  public <T> PCollection<T> parallelDo(IDoFn<S, T> fn, PType<T> type) {
    return parallelDo(null, fn, type);
  }

  @Override
  public <K, V> PTable<K, V> parallelDo(IDoFn<S, Pair<K, V>> fn, PTableType<K, V> type) {
    return parallelDo(null, fn, type);
  }

  @Override
  public <T> PCollection<T> parallelDo(String name, IDoFn<S, T> fn, PType<T> type) {
    return parallelDo(name, fn, type, null);
  }

  @Override
  public <K, V> PTable<K, V> parallelDo(String name, IDoFn<S, Pair<K, V>> fn, PTableType<K, V> type) {
    return parallelDo(name, fn, type, null);
  }

  @Override
  public <T> PCollection<T> parallelDo(String name, IDoFn<S, T> fn, PType<T> type, ParallelDoOptions options) {
    return parallelDo(name, IFnHelpers.wrap(fn), type, options);
  }

  @Override
  public <K, V> PTable<K, V> parallelDo(String name, IDoFn<S, Pair<K, V>> fn, PTableType<K, V> type, ParallelDoOptions options) {
    return parallelDo(name, IFnHelpers.wrap(fn), type, options);
  }

  @Override
  public <T> PCollection<T> flatMap(IFlatMapFn<S, T> fn, PType<T> type) {
    return parallelDo(IFnHelpers.wrapFlatMap(fn), type);
  }

  @Override
  public <K, V> PTable<K, V> flatMap(IFlatMapFn<S, Pair<K, V>> fn, PTableType<K, V> type) {
    return parallelDo(IFnHelpers.wrapFlatMap(fn), type);
  }

  @Override
  public <T> PCollection<T> flatMap(String name, IFlatMapFn<S, T> fn, PType<T> type) {
    return parallelDo(name, IFnHelpers.wrapFlatMap(fn), type);
  }

  @Override
  public <K, V> PTable<K, V> flatMap(String name, IFlatMapFn<S, Pair<K, V>> fn, PTableType<K, V> type) {
    return parallelDo(name, IFnHelpers.wrapFlatMap(fn), type);
  }

  @Override
  public <T> PCollection<T> map(IMapFn<S, T> fn, PType<T> type) {
    return parallelDo(IFnHelpers.wrapMap(fn), type);
  }

  @Override
  public <K, V> PTable<K, V> map(IMapFn<S, Pair<K, V>> fn, PTableType<K, V> type) {
    return parallelDo(IFnHelpers.wrapMap(fn), type);
  }

  @Override
  public <T> PCollection<T> map(String name, IMapFn<S, T> fn, PType<T> type) {
    return parallelDo(name, IFnHelpers.wrapMap(fn), type);
  }

  @Override
  public <K, V> PTable<K, V> map(String name, IMapFn<S, Pair<K, V>> fn, PTableType<K, V> type) {
    return parallelDo(name, IFnHelpers.wrapMap(fn), type);
  }

  public PCollection<S> filter(IFilterFn<S> fn) {
    return filter(IFnHelpers.wrapFilter(fn));
  }

  public PCollection<S> filter(String name, IFilterFn<S> fn) {
    return filter(name, IFnHelpers.wrapFilter(fn));
  }

  @Override
  public PCollection<S> write(Target target) {
    getPipeline().write(this, target);
    return this;
  }

  @Override
  public PCollection<S> write(Target target, Target.WriteMode writeMode) {
    getPipeline().write(this, target, writeMode);
    return this;
  }

  @Override
  public Iterable<S> materialize() {
    return collect;
  }

  @Override
  public PCollection<S> cache() {
    // No-op
    return this;
  }

  @Override
  public PCollection<S> cache(CachingOptions options) {
    // No-op
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public PObject<Collection<S>> asCollection() {
    return new CollectionPObject<S>(this);
  }

  @Override
  public PObject<S> first() { return new FirstElementPObject<S>(this); }

  @Override
  public <Output> Output sequentialDo(String label, PipelineCallable<Output> pipelineCallable) {
    pipelineCallable.dependsOn(label, this);
    return getPipeline().sequentialDo(pipelineCallable);
  }

  @Override
  public ReadableData<S> asReadable(boolean materialize) {
    return new MemReadableData<S>(collect);
  }

  public Collection<S> getCollection() {
    return collect;
  }

  @Override
  public PType<S> getPType() {
    return ptype;
  }

  @Override
  public PTypeFamily getTypeFamily() {
    if (ptype != null) {
      return ptype.getFamily();
    }
    return null;
  }

  @Override
  public long getSize() {
    return collect.isEmpty() ? 0 : 1; // getSize is only used for pipeline optimization in MR
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return collect.toString();
  }

  @Override
  public PTable<S, Long> count() {
    return Aggregate.count(this);
  }

  @Override
  public PObject<Long> length() {
    return Aggregate.length(this);
  }

  @Override
  public PObject<S> max() {
    return Aggregate.max(this);
  }

  @Override
  public PObject<S> min() {
    return Aggregate.min(this);
  }

  @Override
  public PCollection<S> aggregate(Aggregator<S> aggregator) {
    return Aggregate.aggregate(this, aggregator);
  }
  
  @Override
  public PCollection<S> filter(FilterFn<S> filterFn) {
    return parallelDo(filterFn, getPType());
  }

  @Override
  public PCollection<S> filter(String name, FilterFn<S> filterFn) {
    return parallelDo(name, filterFn, getPType());
  }

  @Override
  public <K> PTable<K, S> by(MapFn<S, K> mapFn, PType<K> keyType) {
    return parallelDo(new ExtractKeyFn<K, S>(mapFn), getTypeFamily().tableOf(keyType, getPType()));
  }

  @Override
  public <K> PTable<K, S> by(IMapFn<S, K> mapFn, PType<K> keyType) {
    return parallelDo(new ExtractKeyFn<K, S>(IFnHelpers.wrapMap(mapFn)), getTypeFamily().tableOf(keyType, getPType()));
  }

  @Override
  public <K> PTable<K, S> by(String name, MapFn<S, K> mapFn, PType<K> keyType) {
    return parallelDo(name, new ExtractKeyFn<K, S>(mapFn), getTypeFamily().tableOf(keyType, getPType()));
  }

  /**
   * The method creates a {@link TaskInputOutputContext} that will just provide
   * {@linkplain Configuration}. The method has been implemented with javaassist
   * as there are API changes in versions of Hadoop. In hadoop 1.0.3 the
   * {@linkplain TaskInputOutputContext} is abstract class while in version 2
   * the same is an interface.
   * <p>
   * Note: The intention of this is to provide the bare essentials that are
   * required to make the {@linkplain MemPipeline} work. It lacks even the basic
   * things that can proved some support for unit testing pipeline.
   */
  private static TaskInputOutputContext<?, ?, ?, ?> getInMemoryContext(final Configuration conf) {
    ProxyFactory factory = new ProxyFactory();
    Class<TaskInputOutputContext> superType = TaskInputOutputContext.class;
    Class[] types = new Class[0];
    Object[] args = new Object[0];
    final TaskAttemptID taskAttemptId = new TaskAttemptID();
    if (superType.isInterface()) {
      factory.setInterfaces(new Class[] { superType });
    } else {
      types = new Class[] { Configuration.class, TaskAttemptID.class, RecordWriter.class, OutputCommitter.class,
          StatusReporter.class };
      args = new Object[] { conf, taskAttemptId, null, null, null };
      factory.setSuperclass(superType);
    }

    final Set<String> handledMethods = ImmutableSet.of("getConfiguration", "getCounter", 
                                                  "progress", "getNumReduceTasks", "getTaskAttemptID");
    factory.setFilter(new MethodFilter() {
      @Override
      public boolean isHandled(Method m) {
        return handledMethods.contains(m.getName());
      }
    });
    MethodHandler handler = new MethodHandler() {
      @Override
      public Object invoke(Object arg0, Method m, Method arg2, Object[] args) throws Throwable {
        String name = m.getName();
        if ("getConfiguration".equals(name)) {
          return conf;
        } else if ("progress".equals(name)) {
          // no-op
          return null;
        } else if ("getTaskAttemptID".equals(name)) {
          return taskAttemptId;
        } else if ("getNumReduceTasks".equals(name)) {
          return 1;
        } else if ("getCounter".equals(name)){ // getCounter
          if (args.length == 1) {
            return MemPipeline.getCounters().findCounter((Enum<?>) args[0]);
          } else {
            return MemPipeline.getCounters().findCounter((String) args[0], (String) args[1]);
          }
        } else {
          throw new IllegalStateException("Unhandled method " + name);
        }
      }
    };
    try {
      Object newInstance = factory.create(types, args, handler);
      return (TaskInputOutputContext<?, ?, ?, ?>) newInstance;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
