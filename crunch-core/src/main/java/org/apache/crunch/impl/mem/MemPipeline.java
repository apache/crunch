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
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
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
import org.apache.crunch.types.avro.ReflectDataFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Counters;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class MemPipeline implements Pipeline {

  private static final Log LOG = LogFactory.getLog(MemPipeline.class);
  private static Counters COUNTERS = new Counters();
  private static final MemPipeline INSTANCE = new MemPipeline();

  private int outputIndex = 0;
  
  public static Counters getCounters() {
    return COUNTERS;
  }
  
  public static void clearCounters() {
    COUNTERS = new Counters();
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
    write(collection, target, Target.WriteMode.DEFAULT);
  }
  
  @Override
  public void write(PCollection<?> collection, Target target, Target.WriteMode writeMode) {
    target.handleExisting(writeMode, -1, getConfiguration());
    if (writeMode != Target.WriteMode.APPEND && activeTargets.contains(target)) {
      throw new CrunchRuntimeException("Target " + target
          + " is already written in the current run."
          + " Use WriteMode.APPEND in order to write additional data to it.");
    }
    activeTargets.add(target);
    if (target instanceof PathTarget) {
      Path path = ((PathTarget) target).getPath();
      try {
        FileSystem fs = path.getFileSystem(conf);
        outputIndex++;
        if (target instanceof SeqFileTarget) {
          if (collection instanceof PTable) {
            writeSequenceFileFromPTable(fs, path, (PTable<?, ?>) collection);
          } else {
            writeSequenceFileFromPCollection(fs, path, collection);
          }
        } else {
          FSDataOutputStream os = fs.create(new Path(path, "out" + outputIndex));
          if (target instanceof AvroFileTarget && !(collection instanceof PTable)) {

            writeAvroFile(os, collection.materialize());
          } else {
            LOG.warn("Defaulting to write to a text file from MemPipeline");
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
          }
          os.close();
        }
      } catch (IOException e) {
        LOG.error("Exception writing target: " + target, e);
      }
    } else {
      LOG.error("Target " + target + " is not a PathTarget instance");
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private void writeAvroFile(FSDataOutputStream outputStream, Iterable genericRecords) throws IOException {
    
    Object r = genericRecords.iterator().next();
    
    Schema schema = null;
    
    if (r instanceof GenericContainer) {
      schema = ((GenericContainer) r).getSchema();
    } else {
      schema = new ReflectDataFactory().getReflectData().getSchema(r.getClass());
    }

    GenericDatumWriter genericDatumWriter = new GenericDatumWriter(schema);

    DataFileWriter dataFileWriter = new DataFileWriter(genericDatumWriter);
    dataFileWriter.create(schema, outputStream);

    for (Object record : genericRecords) {
      dataFileWriter.append(record);
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
  public PipelineExecution runAsync() {
    activeTargets.clear();
    return new PipelineExecution() {
      @Override
      public String getPlanDotFile() {
        return "";
      }

      @Override
      public void waitFor(long timeout, TimeUnit timeUnit) throws InterruptedException {
        // no-po
      }

      @Override
      public void waitUntilDone() throws InterruptedException {
        // no-po
      }

      @Override
      public Status getStatus() {
        return Status.SUCCEEDED;
      }

      @Override
      public PipelineResult getResult() {
        return new PipelineResult(ImmutableList.of(new PipelineResult.StageResult("MemPipelineStage", COUNTERS)),
            Status.SUCCEEDED);
      }

      @Override
      public void kill() {
      }
    };
  }
  
  @Override
  public PipelineResult run() {
    activeTargets.clear();
    return new PipelineResult(ImmutableList.of(new PipelineResult.StageResult("MemPipelineStage", COUNTERS)),
        PipelineExecution.Status.SUCCEEDED);
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
}
