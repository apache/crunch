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
package org.apache.crunch.impl.spark;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import javassist.util.proxy.MethodFilter;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.DoFn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkFiles;
import org.apache.spark.broadcast.Broadcast;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SparkRuntimeContext implements Serializable {

  private Broadcast<Configuration> broadConf;
  private Accumulator<Map<String, Long>> counters;
  private transient TaskInputOutputContext context;

  public SparkRuntimeContext(
      Broadcast<Configuration> broadConf,
      Accumulator<Map<String, Long>> counters) {
    this.broadConf = broadConf;
    this.counters = counters;
  }

  public void initialize(DoFn<?, ?> fn) {
    if (context == null) {
      configureLocalFiles();
      context = getTaskIOContext(broadConf, counters);
    }
    fn.setContext(context);
    fn.initialize();
  }

  private void configureLocalFiles() {
    try {
      URI[] uris = DistributedCache.getCacheFiles(getConfiguration());
      if (uris != null) {
        List<String> allFiles = Lists.newArrayList();
        for (URI uri : uris) {
          File f = new File(uri.getPath());
          allFiles.add(SparkFiles.get(f.getName()));
        }
        DistributedCache.setLocalFiles(getConfiguration(), Joiner.on(',').join(allFiles));
      }
    } catch (IOException e) {
      throw new CrunchRuntimeException(e);
    }
  }

  public Configuration getConfiguration() {
    return broadConf.value();
  }

  public static TaskInputOutputContext getTaskIOContext(
      final Broadcast<Configuration> conf,
      final Accumulator<Map<String, Long>> counters) {
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
      args = new Object[] { conf.value(), taskAttemptId, null, null, null };
      factory.setSuperclass(superType);
    }

    final Set<String> handledMethods = ImmutableSet.of("getConfiguration", "getCounter",
        "progress", "getTaskAttemptID");
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
          return conf.value();
        } else if ("progress".equals(name)) {
          // no-op
          return null;
        } else if ("getTaskAttemptID".equals(name)) {
          return taskAttemptId;
        } else if ("getCounter".equals(name)){ // getCounter
          if (args.length == 1) {
            return getCounter(counters, args[0].getClass().getName(), ((Enum) args[0]).name());
          } else {
            return getCounter(counters, (String) args[0], (String) args[1]);
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

  private static Counter getCounter(final Accumulator<Map<String, Long>> accum, final String group,
                                    final String counterName) {
    ProxyFactory factory = new ProxyFactory();
    Class<Counter> superType = Counter.class;
    Class[] types = new Class[0];
    Object[] args = new Object[0];
    if (superType.isInterface()) {
      factory.setInterfaces(new Class[] { superType });
    } else {
      types = new Class[] { String.class, String.class };
      args = new Object[] { group, counterName };
      factory.setSuperclass(superType);
    }

    final Set<String> handledMethods = ImmutableSet.of("getDisplayName", "getName",
        "getValue", "increment", "setValue", "setDisplayName");
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
        if ("increment".equals(name)) {
          accum.add(ImmutableMap.of(group + ":" + counterName, (Long) args[0]));
          return null;
        } else if ("getDisplayName".equals(name)) {
          return counterName;
        } else if ("getName".equals(name)) {
          return counterName;
        } else if ("setDisplayName".equals(name)) {
          // No-op
          return null;
        } else if ("setValue".equals(name)) {
          throw new UnsupportedOperationException("Cannot set counter values in Spark, only increment them");
        } else if ("getValue".equals(name)) {
          throw new UnsupportedOperationException("Cannot read counters during Spark execution");
        } else {
          throw new IllegalStateException("Unhandled method " + name);
        }
      }
    };
    try {
      Object newInstance = factory.create(types, args, handler);
      return (Counter) newInstance;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
