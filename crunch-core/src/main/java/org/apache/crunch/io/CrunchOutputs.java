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

import com.google.common.collect.Sets;
import java.lang.reflect.Method;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.hadoop.mapreduce.TaskAttemptContextFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

  public static void checkOutputSpecs(JobContext jc) throws IOException, InterruptedException {
    Map<String, OutputConfig> outputs = getNamedOutputs(jc.getConfiguration());
    for (Map.Entry<String, OutputConfig> e : outputs.entrySet()) {
      String namedOutput = e.getKey();
      Job job = getJob(jc.getJobID(), e.getKey(), jc.getConfiguration());
      OutputFormat fmt = getOutputFormat(namedOutput, job, e.getValue());
      fmt.checkOutputSpecs(job);
    }
  }

  public static OutputCommitter getOutputCommitter(TaskAttemptContext tac) throws IOException, InterruptedException {
    Map<String, OutputConfig> outputs = getNamedOutputs(tac.getConfiguration());
    Map<String, OutputCommitter> committers = Maps.newHashMap();
    for (Map.Entry<String, OutputConfig> e : outputs.entrySet()) {
      String namedOutput = e.getKey();
      Job job = getJob(tac.getJobID(), e.getKey(), tac.getConfiguration());
      OutputFormat fmt = getOutputFormat(namedOutput, job, e.getValue());
      TaskAttemptContext taskContext = getTaskContext(tac, job);
      OutputCommitter oc = fmt.getOutputCommitter(taskContext);
      committers.put(namedOutput, oc);
    }
    return new CompositeOutputCommitter(outputs, committers);
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
    String serOut = conf.get(CRUNCH_OUTPUTS);
    if (serOut == null || serOut.isEmpty()) {
      return out;
    }
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

  private TaskInputOutputContext<?, ?, K, V> baseContext;
  private Configuration baseConf;
  private final Map<String, OutputConfig> namedOutputs;
  private final Map<String, OutputState<K, V>> outputStates;
  private final boolean disableOutputCounters;

  /**
   * Creates and initializes multiple outputs support,
   * it should be instantiated in the Mapper/Reducer setup method.
   *
   * @param context the TaskInputOutputContext object
   */
  public CrunchOutputs(TaskInputOutputContext<?, ?, K, V> context) {
    this(context.getConfiguration());
    this.baseContext = context;
  }

  public CrunchOutputs(Configuration conf) {
    this.baseConf = conf;
    this.namedOutputs = getNamedOutputs(conf);
    this.outputStates = Maps.newHashMap();
    this.disableOutputCounters = conf.getBoolean(CRUNCH_DISABLE_OUTPUT_COUNTERS, false);
  }

  @SuppressWarnings("unchecked")
  public void write(String namedOutput, K key, V value)
      throws IOException, InterruptedException {
    if (!namedOutputs.containsKey(namedOutput)) {
      throw new IllegalArgumentException("Undefined named output '" +
        namedOutput + "'");
    }
    if (!disableOutputCounters) {
      baseContext.getCounter(COUNTERS_GROUP, namedOutput).increment(1);
    }
    getOutputState(namedOutput).write(key, value);
  }
  
  public void close() throws IOException, InterruptedException {
    for (OutputState<?, ?> out : outputStates.values()) {
      out.close();
    }
  }

  private OutputState<K, V> getOutputState(String namedOutput) throws IOException, InterruptedException {
    OutputState<?, ?> out = outputStates.get(namedOutput);
    if (out != null) {
      return (OutputState<K, V>) out;
    }

    // The following trick leverages the instantiation of a record writer via
    // the job thus supporting arbitrary output formats.
    Job job = getJob(baseContext.getJobID(), namedOutput, baseConf);

    // Get a job with the expected named output.
    job = getJob(job.getJobID(), namedOutput,baseConf);

    OutputFormat<K, V> fmt = getOutputFormat(namedOutput, job, namedOutputs.get(namedOutput));
    TaskAttemptContext taskContext = null;
    RecordWriter<K, V> recordWriter = null;

    if (baseContext != null) {
      taskContext = getTaskContext(baseContext, job);
      recordWriter = fmt.getRecordWriter(taskContext);
    }
    OutputState<K, V> outputState = new OutputState(taskContext, recordWriter);
    this.outputStates.put(namedOutput, outputState);
    return outputState;
  }

  private static Job getJob(JobID jobID, String namedOutput, Configuration baseConf)
      throws IOException {
    Job job = new Job(new Configuration(baseConf));
    job.getConfiguration().set("crunch.namedoutput", namedOutput);
    setJobID(job, jobID, namedOutput);
    return job;
  }

  private static TaskAttemptContext getTaskContext(TaskAttemptContext baseContext, Job job) {

    org.apache.hadoop.mapreduce.TaskAttemptID baseTaskId = baseContext.getTaskAttemptID();

    // Create a task ID context with our specialized job ID.
    org.apache.hadoop.mapreduce.TaskAttemptID  taskId;
    taskId = new org.apache.hadoop.mapreduce.TaskAttemptID(job.getJobID().getJtIdentifier(),
            job.getJobID().getId(),
            baseTaskId.isMap(),
            baseTaskId.getTaskID().getId(),
            baseTaskId.getId());

    return TaskAttemptContextFactory.create(
            job.getConfiguration(), taskId);
  }

  private static void setJobID(Job job, JobID jobID, String namedOutput) {
    Method setJobIDMethod;
    JobID newJobID = jobID;
    try {
      // Hadoop 2
      setJobIDMethod = Job.class.getMethod("setJobID", JobID.class);
      // Add the named output to the job ID, since that is used by some output formats
      // to create temporary outputs.
      newJobID = jobID == null || jobID.getJtIdentifier().contains(namedOutput) ?
          jobID :
          new JobID(jobID.getJtIdentifier() + "_" + namedOutput, jobID.getId());
    } catch (NoSuchMethodException e) {
      // Hadoop 1's setJobID method is package private and declared by JobContext
      try {
        setJobIDMethod = JobContext.class.getDeclaredMethod("setJobID", JobID.class);
      } catch (NoSuchMethodException e1) {
        throw new CrunchRuntimeException(e);
      }
      setJobIDMethod.setAccessible(true);
    }
    try {
      setJobIDMethod.invoke(job, newJobID);
    } catch (Exception e) {
      throw new CrunchRuntimeException("Could not set job ID to " + jobID, e);
    }
  }

  private static void configureJob(
      String namedOutput,
      Job job,
      OutputConfig outConfig) throws IOException {
    job.getConfiguration().set(BASE_OUTPUT_NAME, namedOutput);
    job.setOutputFormatClass(outConfig.bundle.getFormatClass());
    job.setOutputKeyClass(outConfig.keyClass);
    job.setOutputValueClass(outConfig.valueClass);
    outConfig.bundle.configure(job.getConfiguration());
  }

  private static OutputFormat getOutputFormat(
      String namedOutput,
      Job job,
      OutputConfig outConfig) throws IOException {
    configureJob(namedOutput, job, outConfig);
    try {
      return ReflectionUtils.newInstance(
          job.getOutputFormatClass(),
          job.getConfiguration());
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }

  private static class OutputState<K, V> {
    private final TaskAttemptContext context;
    private final RecordWriter<K, V> recordWriter;

    public OutputState(TaskAttemptContext context, RecordWriter<K, V> recordWriter) {
      this.context = context;
      this.recordWriter = recordWriter;
    }

    public void write(K key, V value) throws IOException, InterruptedException {
      recordWriter.write(key, value);
    }

    public void close() throws IOException, InterruptedException {
      recordWriter.close(context);
    }
  }

  private static class CompositeOutputCommitter extends OutputCommitter {

    private final Map<String, OutputConfig> outputs;
    private final Map<String, OutputCommitter> committers;

    public CompositeOutputCommitter(Map<String, OutputConfig> outputs, Map<String, OutputCommitter> committers) {
      this.outputs = outputs;
      this.committers = committers;
    }

    private TaskAttemptContext getContext(String namedOutput, TaskAttemptContext baseContext) throws IOException {
      Job job = getJob(baseContext.getJobID(), namedOutput, baseContext.getConfiguration());
      configureJob(namedOutput, job, outputs.get(namedOutput));

      return getTaskContext(baseContext, job);
    }

    @Override
    public void setupJob(JobContext jobContext) throws IOException {
      Configuration conf = jobContext.getConfiguration();
      for (Map.Entry<String, OutputCommitter> e : committers.entrySet()) {
        Job job = getJob(jobContext.getJobID(), e.getKey(), conf);
        configureJob(e.getKey(), job, outputs.get(e.getKey()));
        e.getValue().setupJob(job);
      }
    }

    @Override
    public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {
      for (Map.Entry<String, OutputCommitter> e : committers.entrySet()) {
        e.getValue().setupTask(getContext(e.getKey(), taskAttemptContext));
      }
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
      for (Map.Entry<String, OutputCommitter> e : committers.entrySet()) {
        if (e.getValue().needsTaskCommit(getContext(e.getKey(), taskAttemptContext))) {
          return true;
        }
      }
      return false;
    }

    @Override
    public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {
      for (Map.Entry<String, OutputCommitter> e : committers.entrySet()) {

        e.getValue().commitTask(getContext(e.getKey(), taskAttemptContext));
      }
    }

    @Override
    public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
      for (Map.Entry<String, OutputCommitter> e : committers.entrySet()) {
        e.getValue().abortTask(getContext(e.getKey(), taskAttemptContext));
      }
    }

    @Override
    public void commitJob(JobContext jobContext) throws IOException {
      Configuration conf = jobContext.getConfiguration();
      Set<Path> handledPaths = Sets.newHashSet();
      for (Map.Entry<String, OutputCommitter> e : committers.entrySet()) {
        OutputCommitter oc = e.getValue();
        Job job = getJob(jobContext.getJobID(), e.getKey(), conf);
        configureJob(e.getKey(), job, outputs.get(e.getKey()));
        if (oc instanceof FileOutputCommitter) {
          Path outputPath = ((FileOutputCommitter) oc).getWorkPath().getParent();
          if (handledPaths.contains(outputPath)) {
            continue;
          } else {
            handledPaths.add(outputPath);
          }
        }
        oc.commitJob(job);
      }
    }

    @Override
    public void abortJob(JobContext jobContext, JobStatus.State state) throws IOException {
      Configuration conf = jobContext.getConfiguration();
      for (Map.Entry<String, OutputCommitter> e : committers.entrySet()) {
        Job job = getJob(jobContext.getJobID(), e.getKey(), conf);
        configureJob(e.getKey(), job, outputs.get(e.getKey()));
        e.getValue().abortJob(job, state);
      }
    }
  }
}
