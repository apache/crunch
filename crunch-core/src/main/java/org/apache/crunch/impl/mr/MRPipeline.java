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
package org.apache.crunch.impl.mr;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineExecution;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Source;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.TableSource;
import org.apache.crunch.Target;
import org.apache.crunch.Target.WriteMode;
import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.impl.mr.collect.InputCollection;
import org.apache.crunch.impl.mr.collect.InputTable;
import org.apache.crunch.impl.mr.collect.PCollectionImpl;
import org.apache.crunch.impl.mr.collect.PGroupedTableImpl;
import org.apache.crunch.impl.mr.collect.UnionCollection;
import org.apache.crunch.impl.mr.collect.UnionTable;
import org.apache.crunch.impl.mr.exec.MRExecutor;
import org.apache.crunch.impl.mr.plan.MSCRPlanner;
import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.crunch.io.From;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.io.ReadableSourceTarget;
import org.apache.crunch.io.To;
import org.apache.crunch.materialize.MaterializableIterable;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Pipeline implementation that is executed within Hadoop MapReduce.
 */
public class MRPipeline implements Pipeline {

  private static final Log LOG = LogFactory.getLog(MRPipeline.class);

  private static final Random RANDOM = new Random();

  private final Class<?> jarClass;
  private final String name;
  private final Map<PCollectionImpl<?>, Set<Target>> outputTargets;
  private final Map<PCollectionImpl<?>, MaterializableIterable<?>> outputTargetsToMaterialize;
  private Path tempDirectory;
  private int tempFileIndex;
  private int nextAnonymousStageId;

  private Configuration conf;

  /**
   * Instantiate with a default Configuration and name.
   * 
   * @param jarClass Class containing the main driver method for running the pipeline
   */
  public MRPipeline(Class<?> jarClass) {
    this(jarClass, new Configuration());
  }

  /**
   * Instantiate with a custom pipeline name. The name will be displayed in the Hadoop JobTracker.
   * 
   * @param jarClass Class containing the main driver method for running the pipeline
   * @param name Display name of the pipeline
   */
  public MRPipeline(Class<?> jarClass, String name) {
    this(jarClass, name, new Configuration());
  }

  /**
   * Instantiate with a custom configuration and default naming.
   * 
   * @param jarClass Class containing the main driver method for running the pipeline
   * @param conf Configuration to be used within all MapReduce jobs run in the pipeline
   */
  public MRPipeline(Class<?> jarClass, Configuration conf) {
    this(jarClass, jarClass.getName(), conf);
  }

  /**
   * Instantiate with a custom name and configuration. The name will be displayed in the Hadoop
   * JobTracker.
   * 
   * @param jarClass Class containing the main driver method for running the pipeline
   * @param name Display name of the pipeline
   * @param conf Configuration to be used within all MapReduce jobs run in the pipeline
   */
  public MRPipeline(Class<?> jarClass, String name, Configuration conf) {
    this.jarClass = jarClass;
    this.name = name;
    this.outputTargets = Maps.newHashMap();
    this.outputTargetsToMaterialize = Maps.newHashMap();
    this.conf = conf;
    this.tempDirectory = createTempDirectory(conf);
    this.tempFileIndex = 0;
    this.nextAnonymousStageId = 0;
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public void setConfiguration(Configuration conf) {
    this.conf = conf;
    this.tempDirectory = createTempDirectory(conf);
  }

  public MRExecutor plan() {
    Map<PCollectionImpl<?>, MaterializableIterable> toMaterialize = Maps.newHashMap();
    for (PCollectionImpl<?> c : outputTargets.keySet()) {
      if (outputTargetsToMaterialize.containsKey(c)) {
        toMaterialize.put(c, outputTargetsToMaterialize.get(c));
        outputTargetsToMaterialize.remove(c);
      }
    }
    MSCRPlanner planner = new MSCRPlanner(this, outputTargets, toMaterialize);
    try {
      return planner.plan(jarClass, conf);
    } catch (IOException e) {
      throw new CrunchRuntimeException(e);
    }
  }

  @Override
  public PipelineResult run() {
    try {
      PipelineExecution pipelineExecution = runAsync();
      pipelineExecution.waitUntilDone();
      return pipelineExecution.getResult();
    } catch (InterruptedException e) {
      // TODO: How to handle this without changing signature?
      LOG.error("Exception running pipeline", e);
      return PipelineResult.EMPTY;
    }
  }
  
  @Override
  public MRPipelineExecution runAsync() {
    MRPipelineExecution res = plan().execute();
    outputTargets.clear();
    return res;
  }

  @Override
  public PipelineResult done() {
    PipelineResult res = null;
    if (!outputTargets.isEmpty()) {
      res = run();
    }
    cleanup();
    return res;
  }

  public <S> PCollection<S> read(Source<S> source) {
    return new InputCollection<S>(source, this);
  }

  public <K, V> PTable<K, V> read(TableSource<K, V> source) {
    return new InputTable<K, V>(source, this);
  }

  public PCollection<String> readTextFile(String pathName) {
    return read(From.textFile(pathName));
  }

  public void write(PCollection<?> pcollection, Target target) {
    write(pcollection, target, Target.WriteMode.DEFAULT);
  }
  
  @SuppressWarnings("unchecked")
  public void write(PCollection<?> pcollection, Target target,
      Target.WriteMode writeMode) {
    if (pcollection instanceof PGroupedTableImpl) {
      pcollection = ((PGroupedTableImpl<?, ?>) pcollection).ungroup();
    } else if (pcollection instanceof UnionCollection || pcollection instanceof UnionTable) {
      pcollection = pcollection.parallelDo("UnionCollectionWrapper",
          (MapFn) IdentityFn.<Object> getInstance(), pcollection.getPType());
    }
    boolean exists = target.handleExisting(writeMode, ((PCollectionImpl) pcollection).getLastModifiedAt(),
        getConfiguration());
    if (exists && writeMode == WriteMode.CHECKPOINT) {
      SourceTarget<?> st = target.asSourceTarget(pcollection.getPType());
      if (st == null) {
        throw new CrunchRuntimeException("Target " + target + " does not support checkpointing");
      } else {
        ((PCollectionImpl) pcollection).materializeAt(st);
      }
      return;
    } else if (writeMode != WriteMode.APPEND && targetInCurrentRun(target)) {
      throw new CrunchRuntimeException("Target " + target + " is already written in current run." +
          " Use WriteMode.APPEND in order to write additional data to it.");
    }
    addOutput((PCollectionImpl<?>) pcollection, target);
  }

  private boolean targetInCurrentRun(Target target) {
    for (Set<Target> targets : outputTargets.values()) {
      if (targets.contains(target)) {
        return true;
      }
    }
    return false;
  }
  
  private void addOutput(PCollectionImpl<?> impl, Target target) {
    if (!outputTargets.containsKey(impl)) {
      outputTargets.put(impl, Sets.<Target> newHashSet());
    }
    outputTargets.get(impl).add(target);
  }

  @Override
  public <T> Iterable<T> materialize(PCollection<T> pcollection) {

    PCollectionImpl<T> pcollectionImpl = toPcollectionImpl(pcollection);
    ReadableSource<T> readableSrc = getMaterializeSourceTarget(pcollectionImpl);

    MaterializableIterable<T> c = new MaterializableIterable<T>(this, readableSrc);
    if (!outputTargetsToMaterialize.containsKey(pcollectionImpl)) {
      outputTargetsToMaterialize.put(pcollectionImpl, c);
    }
    return c;
  }

  /**
   * Retrieve a ReadableSourceTarget that provides access to the contents of a {@link PCollection}.
   * This is primarily intended as a helper method to {@link #materialize(PCollection)}. The
   * underlying data of the ReadableSourceTarget may not be actually present until the pipeline is
   * run.
   * 
   * @param pcollection The collection for which the ReadableSourceTarget is to be retrieved
   * @return The ReadableSourceTarget
   * @throws IllegalArgumentException If no ReadableSourceTarget can be retrieved for the given
   *           PCollection
   */
  public <T> ReadableSource<T> getMaterializeSourceTarget(PCollection<T> pcollection) {
    PCollectionImpl<T> impl = toPcollectionImpl(pcollection);

    // First, check to see if this is a readable input collection.
    if (impl instanceof InputCollection) {
      InputCollection<T> ic = (InputCollection<T>) impl;
      if (ic.getSource() instanceof ReadableSource) {
        return (ReadableSource) ic.getSource();
      } else {
        throw new IllegalArgumentException(
            "Cannot materialize non-readable input collection: " + ic);
      }
    } else if (impl instanceof InputTable) {
      InputTable it = (InputTable) impl;
      if (it.getSource() instanceof ReadableSource) {
        return (ReadableSource) it.getSource();
      } else {
        throw new IllegalArgumentException(
            "Cannot materialize non-readable input table: " + it);
      }
    }

    // Next, check to see if this pcollection has already been materialized.
    SourceTarget<T> matTarget = impl.getMaterializedAt();
    if (matTarget != null && matTarget instanceof ReadableSourceTarget) {
      return (ReadableSourceTarget<T>) matTarget;
    }
    
    // Check to see if we plan on materializing this collection on the
    // next run.
    ReadableSourceTarget<T> srcTarget = null;
    if (outputTargets.containsKey(pcollection)) {
      for (Target target : outputTargets.get(impl)) {
        if (target instanceof ReadableSourceTarget) {
          return (ReadableSourceTarget<T>) target;
        }
      }
    }

    // If we're not planning on materializing it already, create a temporary
    // output to hold the materialized records and return that.
    SourceTarget<T> st = createIntermediateOutput(pcollection.getPType());
    if (!(st instanceof ReadableSourceTarget)) {
      throw new IllegalArgumentException("The PType for the given PCollection is not readable"
          + " and cannot be materialized");
    } else {
      srcTarget = (ReadableSourceTarget<T>) st;
      addOutput(impl, srcTarget);
      return srcTarget;
    }
  }

  /**
   * Safely cast a PCollection into a PCollectionImpl, including handling the case of
   * UnionCollections.
   * 
   * @param pcollection The PCollection to be cast/transformed
   * @return The PCollectionImpl representation
   */
  private <T> PCollectionImpl<T> toPcollectionImpl(PCollection<T> pcollection) {
    PCollectionImpl<T> pcollectionImpl = null;
    if (pcollection instanceof UnionCollection || pcollection instanceof UnionTable) {
      pcollectionImpl = (PCollectionImpl<T>) pcollection.parallelDo("UnionCollectionWrapper",
          (MapFn) IdentityFn.<Object> getInstance(), pcollection.getPType());
    } else {
      pcollectionImpl = (PCollectionImpl<T>) pcollection;
    }
    return pcollectionImpl;
  }

  public <T> SourceTarget<T> createIntermediateOutput(PType<T> ptype) {
    return ptype.getDefaultFileSource(createTempPath());
  }

  public Path createTempPath() {
    tempFileIndex++;
    return new Path(tempDirectory, "p" + tempFileIndex);
  }

  private static Path createTempDirectory(Configuration conf) {
    Path dir = createTemporaryPath(conf);
    try {
      dir.getFileSystem(conf).mkdirs(dir);
    } catch (IOException e) {
      throw new RuntimeException("Cannot create job output directory " + dir, e);
    }
    return dir;
  }

  private static Path createTemporaryPath(Configuration conf) {
    String baseDir = conf.get(RuntimeParameters.TMP_DIR, "/tmp");
    return new Path(baseDir, "crunch-" + (RANDOM.nextInt() & Integer.MAX_VALUE));
  }

  @Override
  public <T> void writeTextFile(PCollection<T> pcollection, String pathName) {
    pcollection.parallelDo("asText", new StringifyFn<T>(), Writables.strings())
        .write(To.textFile(pathName));
  }

  private static class StringifyFn<T> extends MapFn<T, String> {
    @Override
    public String map(T input) {
      return input.toString();
    }
  }
  
  private void cleanup() {
    if (!outputTargets.isEmpty()) {
      LOG.warn("Not running cleanup while output targets remain");
      return;
    }
    try {
      FileSystem fs = tempDirectory.getFileSystem(conf);
      if (fs.exists(tempDirectory)) {
        fs.delete(tempDirectory, true);
      }
    } catch (IOException e) {
      LOG.info("Exception during cleanup", e);
    }
  }

  public int getNextAnonymousStageId() {
    return nextAnonymousStageId++;
  }

  @Override
  public void enableDebug() {
    // Turn on Crunch runtime error catching.
    getConfiguration().setBoolean(RuntimeParameters.DEBUG, true);
  }

  @Override
  public String getName() {
    return name;
  }
}
