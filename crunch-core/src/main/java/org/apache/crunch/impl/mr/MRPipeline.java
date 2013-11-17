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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.crunch.CachingOptions;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.PCollection;
import org.apache.crunch.PipelineExecution;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.dist.DistributedPipeline;
import org.apache.crunch.impl.dist.collect.PCollectionImpl;
import org.apache.crunch.impl.mr.collect.MRCollectionFactory;
import org.apache.crunch.impl.mr.exec.MRExecutor;
import org.apache.crunch.impl.mr.plan.MSCRPlanner;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.materialize.MaterializableIterable;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Maps;

/**
 * Pipeline implementation that is executed within Hadoop MapReduce.
 */
public class MRPipeline extends DistributedPipeline {

  private static final Log LOG = LogFactory.getLog(MRPipeline.class);

  private final Class<?> jarClass;

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
    super(name, conf, new MRCollectionFactory());
    this.jarClass = jarClass;
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
      return planner.plan(jarClass, getConfiguration());
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
  public <T> Iterable<T> materialize(PCollection<T> pcollection) {
    ((PCollectionImpl) pcollection).setBreakpoint();
    ReadableSource<T> readableSrc = getMaterializeSourceTarget(pcollection);
    MaterializableIterable<T> c = new MaterializableIterable<T>(this, readableSrc);
    if (!outputTargetsToMaterialize.containsKey(pcollection)) {
      outputTargetsToMaterialize.put((PCollectionImpl) pcollection, c);
    }
    return c;
  }

  @Override
  public <T> void cache(PCollection<T> pcollection, CachingOptions options) {
    // Identical to materialization in a MapReduce context
    materialize(pcollection);
  }
}
