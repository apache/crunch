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
package org.apache.crunch.impl.mem;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Source;
import org.apache.crunch.TableSource;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mem.collect.MemCollection;
import org.apache.crunch.impl.mem.collect.MemTable;
import org.apache.crunch.io.At;
import org.apache.crunch.io.PathTarget;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class MemPipeline implements Pipeline {

  private static final Log LOG = LogFactory.getLog(MemPipeline.class);
  private static Counters COUNTERS = new Counters();
  private static final MemPipeline INSTANCE = new MemPipeline();

  public static Counters getCounters() {
    return COUNTERS;
  }
  
  public static Pipeline getInstance() {
    return INSTANCE;
  }

  public static <T> PCollection<T> collectionOf(T... ts) {
    return new MemCollection<T>(ImmutableList.copyOf(ts));
  }

  public static <T> PCollection<T> collectionOf(Iterable<T> collect) {
    return new MemCollection<T>(collect);
  }

  public static <T> PCollection<T> typedCollectionOf(PType<T> ptype, T... ts) {
    return new MemCollection<T>(ImmutableList.copyOf(ts), ptype, null);
  }

  public static <T> PCollection<T> typedCollectionOf(PType<T> ptype, Iterable<T> collect) {
    return new MemCollection<T>(collect, ptype, null);
  }

  public static <S, T> PTable<S, T> tableOf(S s, T t, Object... more) {
    List<Pair<S, T>> pairs = Lists.newArrayList();
    pairs.add(Pair.of(s, t));
    for (int i = 0; i < more.length; i += 2) {
      pairs.add(Pair.of((S) more[i], (T) more[i + 1]));
    }
    return new MemTable<S, T>(pairs);
  }

  public static <S, T> PTable<S, T> typedTableOf(PTableType<S, T> ptype, S s, T t, Object... more) {
    List<Pair<S, T>> pairs = Lists.newArrayList();
    pairs.add(Pair.of(s, t));
    for (int i = 0; i < more.length; i += 2) {
      pairs.add(Pair.of((S) more[i], (T) more[i + 1]));
    }
    return new MemTable<S, T>(pairs, ptype, null);
  }

  public static <S, T> PTable<S, T> tableOf(Iterable<Pair<S, T>> pairs) {
    return new MemTable<S, T>(pairs);
  }

  public static <S, T> PTable<S, T> typedTableOf(PTableType<S, T> ptype, Iterable<Pair<S, T>> pairs) {
    return new MemTable<S, T>(pairs, ptype, null);
  }

  private Configuration conf = new Configuration();

  private MemPipeline() {
  }

  @Override
  public void setConfiguration(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public <T> PCollection<T> read(Source<T> source) {
    if (source instanceof ReadableSource) {
      try {
        Iterable<T> iterable = ((ReadableSource<T>) source).read(conf);
        return new MemCollection<T>(iterable, source.getType(), source.toString());
      } catch (IOException e) {
        LOG.error("Exception reading source: " + source.toString(), e);
        throw new IllegalStateException(e);
      }
    }
    LOG.error("Source " + source + " is not readable");
    throw new IllegalStateException("Source " + source + " is not readable");
  }

  @Override
  public <K, V> PTable<K, V> read(TableSource<K, V> source) {
    if (source instanceof ReadableSource) {
      try {
        Iterable<Pair<K, V>> iterable = ((ReadableSource<Pair<K, V>>) source).read(conf);
        return new MemTable<K, V>(iterable, source.getTableType(), source.toString());
      } catch (IOException e) {
        LOG.error("Exception reading source: " + source.toString(), e);
        throw new IllegalStateException(e);
      }
    }
    LOG.error("Source " + source + " is not readable");
    throw new IllegalStateException("Source " + source + " is not readable");
  }

  @Override
  public void write(PCollection<?> collection, Target target) {
    if (target instanceof PathTarget) {
      Path path = ((PathTarget) target).getPath();
      try {
        FileSystem fs = path.getFileSystem(conf);
        FSDataOutputStream os = fs.create(new Path(path, "out"));
        if (collection instanceof PTable) {
          for (Object o : collection.materialize()) {
            Pair p = (Pair) o;
            os.writeBytes(p.first().toString());
            os.writeBytes("\t");
            os.writeBytes(p.second().toString());
            os.writeBytes("\r\n");
          }
        } else {
          for (Object o : collection.materialize()) {
            os.writeBytes(o.toString() + "\r\n");
          }
        }
        os.close();
      } catch (IOException e) {
        LOG.error("Exception writing target: " + target, e);
      }
    } else {
      LOG.error("Target " + target + " is not a PathTarget instance");
    }
  }

  @Override
  public PCollection<String> readTextFile(String pathName) {
    return read(At.textFile(pathName));
  }

  @Override
  public <T> void writeTextFile(PCollection<T> collection, String pathName) {
    write(collection, At.textFile(pathName));
  }

  @Override
  public <T> Iterable<T> materialize(PCollection<T> pcollection) {
    return pcollection.materialize();
  }

  @Override
  public PipelineResult run() {
    return PipelineResult.EMPTY;
  }

  @Override
  public PipelineResult done() {
    return PipelineResult.EMPTY;
  }

  @Override
  public void enableDebug() {
    LOG.info("Note: in-memory pipelines do not have debug logging");
  }

  @Override
  public String getName() {
    return "Memory Pipeline";
  }
}
