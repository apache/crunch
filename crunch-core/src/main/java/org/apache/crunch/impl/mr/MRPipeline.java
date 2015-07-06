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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.crunch.CachingOptions;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.PCollection;
import org.apache.crunch.PipelineExecution;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.hadoop.mapreduce.lib.jobcontrol.CrunchControlledJob;
import org.apache.crunch.impl.dist.DistributedPipeline;
import org.apache.crunch.impl.dist.collect.PCollectionImpl;
import org.apache.crunch.impl.mr.collect.MRCollectionFactory;
import org.apache.crunch.impl.mr.exec.MRExecutor;
import org.apache.crunch.impl.mr.plan.MSCRPlanner;
import org.apache.crunch.impl.mr.plan.PlanningParameters;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.materialize.MaterializableIterable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pipeline implementation that is executed within Hadoop MapReduce.
 */
public class MRPipeline extends DistributedPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(MRPipeline.class);

  private final Class<?> jarClass;
  private final List<CrunchControlledJob.Hook> prepareHooks;
  private final List<CrunchControlledJob.Hook> completionHooks;

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
    this.prepareHooks = Lists.newArrayList();
    this.completionHooks = Lists.newArrayList();
  }

  public MRPipeline addPrepareHook(CrunchControlledJob.Hook hook) {
    this.prepareHooks.add(hook);
    return this;
  }

  public List<CrunchControlledJob.Hook> getPrepareHooks() {
    return prepareHooks;
  }

  public MRPipeline addCompletionHook(CrunchControlledJob.Hook hook) {
    this.completionHooks.add(hook);
    return this;
  }

  public List<CrunchControlledJob.Hook> getCompletionHooks() {
    return completionHooks;
  }

  public MRExecutor plan() {
    Map<PCollectionImpl<?>, MaterializableIterable> toMaterialize = Maps.newHashMap();
    for (PCollectionImpl<?> c : outputTargets.keySet()) {
      if (outputTargetsToMaterialize.containsKey(c)) {
        toMaterialize.put(c, outputTargetsToMaterialize.get(c));
        outputTargetsToMaterialize.remove(c);
      }
    }
    MSCRPlanner planner = new MSCRPlanner(this, outputTargets, toMaterialize, appendedTargets, allPipelineCallables);
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
    MRExecutor mrExecutor = plan();
    for (Entry<String, String> dotEntry: mrExecutor.getNamedDotFiles().entrySet()){
      writePlanDotFile(dotEntry.getKey(), dotEntry.getValue());
    }
    MRPipelineExecution res = mrExecutor.execute();
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

  /**
   * Writes the MR job plan dot file contents to a timestamped file if the PIPELINE_DOTFILE_OUTPUT_DIR
   * config key is set with an output directory.
   *
   * @param dotFileContents contents to be written to the dot file
   */
  private void writePlanDotFile(String fileName, String dotFileContents) {
    String dotFileDir = getConfiguration().get(PlanningParameters.PIPELINE_DOTFILE_OUTPUT_DIR);
    if (dotFileDir != null) {
      FSDataOutputStream outputStream = null;
      Exception thrownException = null;
      try {
        URI uri = new URI(dotFileDir);
        FileSystem fs = FileSystem.get(uri, getConfiguration());
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH.mm.ss.SSS");
        String filenameSuffix = String.format("_%s_%s.dot", dateFormat.format(new Date()), fileName);
        String encodedName = URLEncoder.encode(getName(), "UTF-8");
        // We limit the pipeline name to the first 150 characters to keep the output dotfile length less 
        // than 200, as it's not clear what the exact limits are on the filesystem we're writing to (this
        // might be HDFS or it might be a local filesystem)
        final int maxPipeNameLength = 150;
        String filenamePrefix = encodedName.substring(0, Math.min(maxPipeNameLength, encodedName.length()));
        Path jobPlanPath = new Path(uri.getPath(), filenamePrefix + filenameSuffix);
        LOG.info("Writing jobplan to {}", jobPlanPath);
        outputStream = fs.create(jobPlanPath, true);
        outputStream.write(dotFileContents.getBytes(Charsets.UTF_8));
      } catch (URISyntaxException e) {
        thrownException = e;
        throw new CrunchRuntimeException("Invalid dot file dir URI, job plan will not be written: " + dotFileDir, e);
      } catch (IOException e) {
        thrownException = e;
        throw new CrunchRuntimeException("Error writing dotfile contents to " + dotFileDir, e);
      } catch (RuntimeException e) {
        thrownException = e;
        throw e;
      } finally {
        if (outputStream != null) {
          try {
            outputStream.close();
          } catch (IOException e) {
            if (thrownException == null)
              throw new CrunchRuntimeException("Error closing dotfile", e);
          }
        }
      }
    }
  }

}
