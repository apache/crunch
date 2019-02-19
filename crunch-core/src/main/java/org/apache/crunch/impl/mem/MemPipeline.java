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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Charsets;

import com.google.common.collect.Iterables;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.crunch.CachingOptions;
import org.apache.crunch.CreateOptions;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineCallable;
import org.apache.crunch.PipelineExecution;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Source;
import org.apache.crunch.TableSource;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mem.collect.MemCollection;
import org.apache.crunch.impl.mem.collect.MemTable;
import org.apache.crunch.io.At;
import org.apache.crunch.io.PathTarget;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.io.avro.AvroFileTarget;
import org.apache.crunch.io.seq.SeqFileTarget;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Counters;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemPipeline implements Pipeline {

  private static final Logger LOG = LoggerFactory.getLogger(MemPipeline.class);
  private static Counters COUNTERS = new CountersWrapper();
  private static final MemPipeline INSTANCE = new MemPipeline();

  private int outputIndex = 0;

  public static Counters getCounters() {
    return COUNTERS;
  }

  public static void clearCounters() {
    COUNTERS = new CountersWrapper();
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
  private Set<Target> activeTargets = Sets.newHashSet();

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
    return read(source, null);
  }

  @Override
  public <T> PCollection<T> read(Source<T> source, String named) {
    String name = named == null ? source.toString() : named;
    if (source instanceof ReadableSource) {
      try {
        Iterable<T> iterable = ((ReadableSource<T>) source).read(conf);
        return new MemCollection<T>(iterable, source.getType(), name);
      } catch (IOException e) {
        LOG.error("Exception reading source: " + name, e);
        throw new IllegalStateException(e);
      }
    }
    LOG.error("Source {} is not readable", name);
    throw new IllegalStateException("Source " + name + " is not readable");
  }

  @Override
  public <K, V> PTable<K, V> read(TableSource<K, V> source) {
    return read(source, null);
  }

  @Override
  public <K, V> PTable<K, V> read(TableSource<K, V> source, String named) {
    String name = named == null ? source.toString() : named;
    if (source instanceof ReadableSource) {
      try {
        Iterable<Pair<K, V>> iterable = ((ReadableSource<Pair<K, V>>) source).read(conf);
        return new MemTable<K, V>(iterable, source.getTableType(), name);
      } catch (IOException e) {
        LOG.error("Exception reading source: " + name, e);
        throw new IllegalStateException(e);
      }
    }
    LOG.error("Source {} is not readable", name);
    throw new IllegalStateException("Source " + name + " is not readable");
  }

  @Override
  public void write(PCollection<?> collection, Target target) {
    write(collection, target, Target.WriteMode.DEFAULT);
  }

  @Override
  public void write(PCollection<?> collection, Target target, Target.WriteMode writeMode) {
    // Last modified time does not need to be retrieved for this
    // pipeline implementation
    target.handleExisting(writeMode, -1, getConfiguration());
    if (writeMode != Target.WriteMode.APPEND && activeTargets.contains(target)) {
      throw new CrunchRuntimeException("Target " + target
          + " is already written in the current run."
          + " Use WriteMode.APPEND in order to write additional data to it.");
    }
    activeTargets.add(target);
    if (target instanceof PathTarget) {
      if (collection.getPType() != null) {
        collection.getPType().initialize(getConfiguration());
      }
      Path path = ((PathTarget) target).getPath();
      try {
        FileSystem fs = path.getFileSystem(conf);
        outputIndex++;
        if (target instanceof SeqFileTarget) {
          Path outputPath = new Path(path, "out" + outputIndex + ".seq");
          if (collection instanceof PTable) {
            writeSequenceFileFromPTable(fs, outputPath, (PTable<?, ?>) collection);
          } else {
            writeSequenceFileFromPCollection(fs, outputPath, collection);
          }
        } else {
          if (target instanceof AvroFileTarget){
            Path outputPath = new Path(path, "out" + outputIndex + ".avro");
            FSDataOutputStream os = fs.create(outputPath);
            writeAvroFile(os, collection);
            os.close();
          } else {
            LOG.warn("Defaulting to write to a text file from MemPipeline");
            Path outputPath = new Path(path, "out" + outputIndex + ".txt");
            FSDataOutputStream os = fs.create(outputPath);
            byte[] newLine = "\r\n".getBytes(Charsets.UTF_8);
            if (collection instanceof PTable) {
              byte[] tab = "\t".getBytes(Charsets.UTF_8);
              for (Object o : collection.materialize()) {
                Pair p = (Pair) o;
                os.write(p.first().toString().getBytes(Charsets.UTF_8));
                os.write(tab);
                os.write(p.second().toString().getBytes(Charsets.UTF_8));
                os.write(newLine);
              }
            } else {
              for (Object o : collection.materialize()) {
                os.write(o.toString().getBytes(Charsets.UTF_8));
                os.write(newLine);
              }
            }
            os.close();
          }
        }
      } catch (IOException e) {
        LOG.error("Exception writing target: " + target, e);
      }
    } else {
      LOG.error("Target {} is not a PathTarget instance", target);
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private void writeAvroFile(FSDataOutputStream outputStream, PCollection recordCollection) throws IOException {

    AvroType avroType = (AvroType)recordCollection.getPType();
    if (avroType == null) {
      throw new IllegalStateException("Can't write a non-typed Avro collection");
    }
    DatumWriter datumWriter = Avros.newWriter((AvroType)recordCollection.getPType());
    DataFileWriter dataFileWriter = new DataFileWriter(datumWriter);
    dataFileWriter.create(avroType.getSchema(), outputStream);

    for (Object record : recordCollection.materialize()) {
      dataFileWriter.append(avroType.getOutputMapFn().map(record));
    }

    dataFileWriter.close();
    outputStream.close();
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private void writeSequenceFileFromPTable(final FileSystem fs, final Path path, final PTable table)
      throws IOException {
    final PTableType pType = table.getPTableType();
    final Class<?> keyClass = pType.getConverter().getKeyClass();
    final Class<?> valueClass = pType.getConverter().getValueClass();

    final SequenceFile.Writer writer = new SequenceFile.Writer(fs, fs.getConf(), path, keyClass,
        valueClass);

    for (final Object o : table.materialize()) {
      final Pair<?,?> p = (Pair) o;
      final Object key = pType.getKeyType().getOutputMapFn().map(p.first());
      final Object value = pType.getValueType().getOutputMapFn().map(p.second());
      writer.append(key, value);
    }

    writer.close();
  }

  private void writeSequenceFileFromPCollection(final FileSystem fs, final Path path,
      final PCollection collection) throws IOException {
    final PType pType = collection.getPType();
    final Converter converter = pType.getConverter();
    final Class valueClass = converter.getValueClass();

    final SequenceFile.Writer writer = new SequenceFile.Writer(fs, fs.getConf(), path,
        NullWritable.class, valueClass);

    for (final Object o : collection.materialize()) {
      final Object value = pType.getOutputMapFn().map(o);
      writer.append(NullWritable.get(), value);
    }

    writer.close();
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
  public <T> void cache(PCollection<T> pcollection, CachingOptions options) {
    // No-op
  }

  @Override
  public <T> PCollection<T> emptyPCollection(PType<T> ptype) {
    return typedCollectionOf(ptype, ImmutableList.<T>of());
  }

  @Override
  public <K, V> PTable<K, V> emptyPTable(PTableType<K, V> ptype) {
    return typedTableOf(ptype, ImmutableList.<Pair<K, V>>of());
  }

  @Override
  public <T> PCollection<T> create(Iterable<T> contents, PType<T> ptype) {
    return create(contents, ptype, CreateOptions.none());
  }

  @Override
  public <T> PCollection<T> create(Iterable<T> iterable, PType<T> ptype, CreateOptions options) {
    return typedCollectionOf(ptype, iterable);
  }

  @Override
  public <K, V> PTable<K, V> create(Iterable<Pair<K, V>> contents, PTableType<K, V> ptype) {
    return create(contents, ptype, CreateOptions.none());
  }

  @Override
  public <K, V> PTable<K, V> create(Iterable<Pair<K, V>> contents, PTableType<K, V> ptype, CreateOptions options) {
    return typedTableOf(ptype, contents);
  }

  @Override
  public <S> PCollection<S> union(List<PCollection<S>> collections) {
    List<S> output = Lists.newArrayList();
    for (PCollection<S> pcollect : collections) {
      Iterables.addAll(output, pcollect.materialize());
    }
    return new MemCollection<S>(output, collections.get(0).getPType());
  }

  @Override
  public <K, V> PTable<K, V> unionTables(List<PTable<K, V>> tables) {
    List<Pair<K, V>> values = Lists.newArrayList();
    for (PTable<K, V> table : tables) {
      Iterables.addAll(values, table.materialize());
    }
    return new MemTable<K, V>(values, tables.get(0).getPTableType(), null);
  }

  @Override
  public <Output> Output sequentialDo(PipelineCallable<Output> callable) {
    Output out = callable.generateOutput(this);
    try {
      if (PipelineCallable.Status.FAILURE == callable.call()) {
        throw new IllegalStateException("PipelineCallable " + callable + " failed in in-memory Crunch pipeline");
      }
    } catch (Throwable t) {
      t.printStackTrace();
    }
    return out;
  }

  @Override
  public PipelineExecution runAsync() {
    activeTargets.clear();
    return new MemExecution();
  }

  @Override
  public PipelineResult run() {
    try {
      return runAsync().get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void cleanup(boolean force) {
    //no-op
  }

    @Override
  public PipelineResult done() {
    return run();
  }

  @Override
  public void enableDebug() {
    LOG.info("Note: in-memory pipelines do not have debug logging");
  }

  @Override
  public String getName() {
    return "Memory Pipeline";
  }

  private static class MemExecution extends AbstractFuture<PipelineResult> implements PipelineExecution {

    private PipelineResult res;

    public MemExecution() {
      this.res = new PipelineResult(
          ImmutableList.of(new PipelineResult.StageResult("MemPipelineStage", COUNTERS)),
          PipelineExecution.Status.SUCCEEDED);
    }

    @Override
    public String getPlanDotFile() {
      return "";
    }

    @Override
    public Map<String, String> getNamedDotFiles() {
      return ImmutableMap.of("", "");
    }

    @Override
    public void waitFor(long timeout, TimeUnit timeUnit) throws InterruptedException {
      set(res);
    }

    @Override
    public void waitUntilDone() throws InterruptedException {
      set(res);
    }

    @Override
    public PipelineResult get() throws ExecutionException, InterruptedException {
      set(res);
      return super.get();
    }

    @Override
    public PipelineResult get(long timeout, TimeUnit timeUnit) throws InterruptedException, ExecutionException,
        TimeoutException {
      set(res);
      return super.get(timeout, timeUnit);
    }

    @Override
    public Status getStatus() {
      return isDone() ? Status.SUCCEEDED : Status.READY;
    }

    @Override
    public PipelineResult getResult() {
      return isDone() ? res : null;
    }

    @Override
    public void kill() {
      // No-op
    }
  }
}
