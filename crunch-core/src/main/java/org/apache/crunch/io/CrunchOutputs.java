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
package org.apache.crunch.io;

import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.hadoop.mapreduce.TaskAttemptContextFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * An analogue of {@link CrunchInputs} for handling multiple {@code OutputFormat} instances
 * writing to multiple files within a single MapReduce job.
 */
public class CrunchOutputs<K, V> {
  public static final String CRUNCH_OUTPUTS = "crunch.outputs.dir";
  public static final String CRUNCH_DISABLE_OUTPUT_COUNTERS = "crunch.disable.output.counters";

  private static final char RECORD_SEP = ',';
  private static final char FIELD_SEP = ';';
  private static final Joiner JOINER = Joiner.on(FIELD_SEP);
  private static final Splitter SPLITTER = Splitter.on(FIELD_SEP);

  public static void addNamedOutput(Job job, String name,
      Class<? extends OutputFormat> outputFormatClass,
      Class keyClass, Class valueClass) {
    addNamedOutput(job, name, FormatBundle.forOutput(outputFormatClass), keyClass, valueClass);
  }
  
  public static void addNamedOutput(Job job, String name,
      FormatBundle<? extends OutputFormat> outputBundle,
      Class keyClass, Class valueClass) {
    Configuration conf = job.getConfiguration();
    String inputs = JOINER.join(name, outputBundle.serialize(), keyClass.getName(), valueClass.getName());
    String existing = conf.get(CRUNCH_OUTPUTS);
    conf.set(CRUNCH_OUTPUTS, existing == null ? inputs : existing + RECORD_SEP + inputs);
  }

  public static class OutputConfig<K, V> {
    public FormatBundle<OutputFormat<K, V>> bundle;
    public Class<K> keyClass;
    public Class<V> valueClass;
    
    public OutputConfig(FormatBundle<OutputFormat<K, V>> bundle,
        Class<K> keyClass, Class<V> valueClass) {
      this.bundle = bundle;
      this.keyClass = keyClass;
      this.valueClass = valueClass;
    }
  }

  private static Map<String, OutputConfig> getNamedOutputs(
      TaskInputOutputContext<?, ?, ?, ?> context) {
    return getNamedOutputs(context.getConfiguration());
  }

  public static Map<String, OutputConfig> getNamedOutputs(Configuration conf) {
    Map<String, OutputConfig> out = Maps.newHashMap();
    for (String input : Splitter.on(RECORD_SEP).split(conf.get(CRUNCH_OUTPUTS))) {
      List<String> fields = Lists.newArrayList(SPLITTER.split(input));
      String name = fields.get(0);
      FormatBundle<OutputFormat> bundle = FormatBundle.fromSerialized(fields.get(1), conf);
      try {
        Class<?> keyClass = Class.forName(fields.get(2));
        Class<?> valueClass = Class.forName(fields.get(3));
        out.put(name, new OutputConfig(bundle, keyClass, valueClass));
      } catch (ClassNotFoundException e) {
        throw new CrunchRuntimeException(e);
      }
    }
    return out;
  }
  private static final String BASE_OUTPUT_NAME = "mapreduce.output.basename";
  private static final String COUNTERS_GROUP = CrunchOutputs.class.getName();

  private final TaskInputOutputContext<?, ?, K, V> baseContext;
  private final Map<String, OutputConfig> namedOutputs;
  private final Map<String, RecordWriter<K, V>> recordWriters;
  private final Map<String, TaskAttemptContext> taskContextCache;
  private final boolean disableOutputCounters;

  /**
   * Creates and initializes multiple outputs support,
   * it should be instantiated in the Mapper/Reducer setup method.
   *
   * @param context the TaskInputOutputContext object
   */
  public CrunchOutputs(TaskInputOutputContext<?, ?, K, V> context) {
    this.baseContext = context;
    namedOutputs = getNamedOutputs(context);
    recordWriters = Maps.newHashMap();
    taskContextCache = Maps.newHashMap();
    this.disableOutputCounters = context.getConfiguration().getBoolean(CRUNCH_DISABLE_OUTPUT_COUNTERS, false);
  }

  @SuppressWarnings("unchecked")
  public void write(String namedOutput, K key, V value)
      throws IOException, InterruptedException {
    if (!namedOutputs.containsKey(namedOutput)) {
      throw new IllegalArgumentException("Undefined named output '" +
        namedOutput + "'");
    }
    TaskAttemptContext taskContext = getContext(namedOutput);
    if (!disableOutputCounters) {
      baseContext.getCounter(COUNTERS_GROUP, namedOutput).increment(1);
    }
    getRecordWriter(taskContext, namedOutput).write(key, value);
  }
  
  public void close() throws IOException, InterruptedException {
    for (RecordWriter<?, ?> writer : recordWriters.values()) {
      writer.close(baseContext);
    }
  }
  
  private TaskAttemptContext getContext(String nameOutput) throws IOException {
    TaskAttemptContext taskContext = taskContextCache.get(nameOutput);
    if (taskContext != null) {
      return taskContext;
    }

    // The following trick leverages the instantiation of a record writer via
    // the job thus supporting arbitrary output formats.
    OutputConfig outConfig = namedOutputs.get(nameOutput);
    Configuration conf = new Configuration(baseContext.getConfiguration());
    Job job = new Job(conf);
    job.getConfiguration().set("crunch.namedoutput", nameOutput);
    job.setOutputFormatClass(outConfig.bundle.getFormatClass());
    job.setOutputKeyClass(outConfig.keyClass);
    job.setOutputValueClass(outConfig.valueClass);
    outConfig.bundle.configure(job.getConfiguration());
    taskContext = TaskAttemptContextFactory.create(
      job.getConfiguration(), baseContext.getTaskAttemptID());

    taskContextCache.put(nameOutput, taskContext);
    return taskContext;
  }

  private synchronized RecordWriter<K, V> getRecordWriter(
      TaskAttemptContext taskContext, String namedOutput)
      throws IOException, InterruptedException {
    // look for record-writer in the cache
    RecordWriter<K, V> writer = recordWriters.get(namedOutput);

    // If not in cache, create a new one
    if (writer == null) {
      // get the record writer from context output format
      taskContext.getConfiguration().set(BASE_OUTPUT_NAME, namedOutput);
      try {
        OutputFormat format = ReflectionUtils.newInstance(
            taskContext.getOutputFormatClass(),
            taskContext.getConfiguration());
        writer = format.getRecordWriter(taskContext);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
      recordWriters.put(namedOutput, writer);
    }

    return writer;
  }
}
