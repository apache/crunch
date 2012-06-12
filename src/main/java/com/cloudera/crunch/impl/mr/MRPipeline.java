/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 * 
 */
package com.cloudera.crunch.impl.mr;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.PipelineResult;
import com.cloudera.crunch.Source;
import com.cloudera.crunch.SourceTarget;
import com.cloudera.crunch.TableSource;
import com.cloudera.crunch.Target;
import com.cloudera.crunch.fn.IdentityFn;
import com.cloudera.crunch.impl.mr.collect.InputCollection;
import com.cloudera.crunch.impl.mr.collect.InputTable;
import com.cloudera.crunch.impl.mr.collect.PCollectionImpl;
import com.cloudera.crunch.impl.mr.collect.PGroupedTableImpl;
import com.cloudera.crunch.impl.mr.collect.UnionCollection;
import com.cloudera.crunch.impl.mr.collect.UnionTable;
import com.cloudera.crunch.impl.mr.plan.MSCRPlanner;
import com.cloudera.crunch.impl.mr.run.RuntimeParameters;
import com.cloudera.crunch.io.At;
import com.cloudera.crunch.io.ReadableSourceTarget;
import com.cloudera.crunch.materialize.MaterializableIterable;
import com.cloudera.crunch.types.PType;
import com.cloudera.crunch.types.writable.WritableTypeFamily;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class MRPipeline implements Pipeline {

  private static final Log LOG = LogFactory.getLog(MRPipeline.class);
  
  private static final Random RANDOM = new Random();
  
  private final Class<?> jarClass;
  private final String name;
  private final Map<PCollectionImpl, Set<Target>> outputTargets;
  private final Map<PCollectionImpl, MaterializableIterable> outputTargetsToMaterialize;
  private final Path tempDirectory;
  private int tempFileIndex;
  private int nextAnonymousStageId;

  private Configuration conf;

  public MRPipeline(Class<?> jarClass) throws IOException {
    this(jarClass, new Configuration());
  }
  
  public MRPipeline(Class<?> jarClass, String name){
    this(jarClass, name, new Configuration());
  }
  
  public MRPipeline(Class<?> jarClass, Configuration conf) {
	  this(jarClass, jarClass.getName(), conf);
  }
  
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
  }
  
  @Override
  public PipelineResult run() {
    MSCRPlanner planner = new MSCRPlanner(this, outputTargets);
    PipelineResult res = null;
    try {
      res = planner.plan(jarClass, conf).execute();
    } catch (IOException e) {
      LOG.error(e);
      return PipelineResult.EMPTY;
    }
    for (PCollectionImpl c : outputTargets.keySet()) {
      if (outputTargetsToMaterialize.containsKey(c)) {
        MaterializableIterable iter = outputTargetsToMaterialize.get(c);
        iter.materialize();
        c.materializeAt(iter.getSourceTarget());
        outputTargetsToMaterialize.remove(c);
      } else {
        boolean materialized = false;
        for (Target t : outputTargets.get(c)) {
          if (!materialized && t instanceof Source) {
           c.materializeAt((SourceTarget) t);
           materialized = true;
          }
        }
      }
    }
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
    return read(At.textFile(pathName));
  }

  @SuppressWarnings("unchecked")
  public void write(PCollection<?> pcollection, Target target) {
    if (pcollection instanceof PGroupedTableImpl) {
      pcollection = ((PGroupedTableImpl) pcollection).ungroup();
    } else if (pcollection instanceof UnionCollection || pcollection instanceof UnionTable) {
      pcollection = pcollection.parallelDo("UnionCollectionWrapper",  
    		  (MapFn)IdentityFn.<Object>getInstance(), pcollection.getPType());	 
    }
    addOutput((PCollectionImpl) pcollection, target);
  }

  private void addOutput(PCollectionImpl impl, Target target) {
    if (!outputTargets.containsKey(impl)) {
      outputTargets.put(impl, Sets.<Target>newHashSet());
    }
    outputTargets.get(impl).add(target);
  }
  
  @Override
  public <T> Iterable<T> materialize(PCollection<T> pcollection) {
	  
    if (pcollection instanceof UnionCollection) {
    	pcollection = pcollection.parallelDo("UnionCollectionWrapper",  
	        (MapFn)IdentityFn.<Object>getInstance(), pcollection.getPType());	 
	}  
    PCollectionImpl impl = (PCollectionImpl) pcollection;
    SourceTarget<T> matTarget = impl.getMaterializedAt();
    if (matTarget != null && matTarget instanceof ReadableSourceTarget) {
      return new MaterializableIterable<T>(this, (ReadableSourceTarget<T>) matTarget);
    }
    
	ReadableSourceTarget<T> srcTarget = null;
	if (outputTargets.containsKey(pcollection)) {
	  for (Target target : outputTargets.get(impl)) {
	    if (target instanceof ReadableSourceTarget) {
		  srcTarget = (ReadableSourceTarget) target;
		  break;
	    }
	  }
	}
	
	if (srcTarget == null) {
	  SourceTarget<T> st = createIntermediateOutput(pcollection.getPType());
	  if (!(st instanceof ReadableSourceTarget)) {
		throw new IllegalArgumentException("The PType for the given PCollection is not readable"
		    + " and cannot be materialized");
	  } else {
		srcTarget = (ReadableSourceTarget) st;
		addOutput(impl, srcTarget);
	  }
	}
	
	MaterializableIterable<T> c = new MaterializableIterable<T>(this, srcTarget);
	outputTargetsToMaterialize.put(impl, c);
	return c;
  }

  public <T> SourceTarget<T> createIntermediateOutput(PType<T> ptype) {
	return ptype.getDefaultFileSource(createTempPath());
  }

  public Path createTempPath() {
    tempFileIndex++;
    return new Path(tempDirectory, "p" + tempFileIndex);
  }
  
  private static Path createTempDirectory(Configuration conf) {
    Path dir = new Path("/tmp/crunch" + RANDOM.nextInt());
	try {
	  FileSystem.get(conf).mkdirs(dir);
	} catch (IOException e) {
	  LOG.error("Exception creating job output directory", e);
	  throw new RuntimeException(e);
	}
    return dir;
  }
  
  @Override
  public <T> void writeTextFile(PCollection<T> pcollection, String pathName) {
    // Ensure that this is a writable pcollection instance.
    pcollection = pcollection.parallelDo("asText", IdentityFn.<T>getInstance(),
        WritableTypeFamily.getInstance().as(pcollection.getPType()));
    write(pcollection, At.textFile(pathName));
  }

  private void cleanup() {
    if (!outputTargets.isEmpty()) {
      LOG.warn("Not running cleanup while output targets remain");
      return;
    }
    try {
      FileSystem fs = FileSystem.get(conf);
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
	getConfiguration().setBoolean(RuntimeParameters.DEBUG, true);
  }
  
  @Override
  public String getName() {
	  return name;
  }
}
