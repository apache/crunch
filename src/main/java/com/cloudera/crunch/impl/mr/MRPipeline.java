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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.Source;
import com.cloudera.crunch.SourceTarget;
import com.cloudera.crunch.TableSource;
import com.cloudera.crunch.Target;
import com.cloudera.crunch.fn.IdentityFn;
import com.cloudera.crunch.impl.mr.collect.InputCollection;
import com.cloudera.crunch.impl.mr.collect.InputTable;
import com.cloudera.crunch.impl.mr.collect.PCollectionImpl;
import com.cloudera.crunch.impl.mr.collect.PGroupedTableImpl;
import com.cloudera.crunch.impl.mr.plan.MSCRPlanner;
import com.cloudera.crunch.io.ReadableSourceTarget;
import com.cloudera.crunch.io.text.TextFileSourceTarget;
import com.cloudera.crunch.materialize.MaterializableIterable;
import com.cloudera.crunch.type.PType;
import com.cloudera.crunch.type.writable.WritableTypeFamily;
import com.google.common.collect.Maps;

public class MRPipeline implements Pipeline {

  private static final Log LOG = LogFactory.getLog(MRPipeline.class);
  
  private static final Random RANDOM = new Random();
  
  private final Class<?> jarClass;
  private final Map<PCollectionImpl, Target> outputTargets;
  private final Map<PCollectionImpl, MaterializableIterable> outputTargetsToMaterialize;
  private final Configuration conf;
  private final Path tempDirectory;
  private int tempFileIndex;
  private int nextAnonymousStageId;
  
  public MRPipeline(Class<?> jarClass) throws IOException {
    this(jarClass, new Configuration());
  }
  
  public MRPipeline(Class<?> jarClass, Configuration conf) throws IOException {
    this.jarClass = jarClass;
    this.outputTargets = Maps.newHashMap();
    this.outputTargetsToMaterialize = Maps.newHashMap();
    this.conf = conf;
    this.tempDirectory = createTempDirectory(conf);
    this.tempFileIndex = 0;
    this.nextAnonymousStageId = 0;
  }

  private static Path createTempDirectory(Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path dir = new Path("/tmp/crunch" + RANDOM.nextInt());
    fs.mkdirs(dir);
    return dir;
  }
  
  public void run() {
    MSCRPlanner planner = new MSCRPlanner(this, outputTargets);
    try {
      planner.plan(jarClass, conf).execute();
    } catch (IOException e) {
      LOG.error(e);
      return;
    }
    for (Map.Entry<PCollectionImpl, Target> e : outputTargets.entrySet()) {
      if (e.getValue() instanceof Source) {
        e.getKey().materializeAt((Source) e.getValue());
      }
      
      if (outputTargetsToMaterialize.containsKey(e.getKey())) {
    	MaterializableIterable c = outputTargetsToMaterialize.get(e.getKey());
    	c.materialize();
    	outputTargetsToMaterialize.remove(e.getKey());
      }
    }
    outputTargets.clear();
  }

  @Override
  public void done() {
    if (!outputTargets.isEmpty()) {
      run();
    }
    cleanup();
  }
  
  public <S> PCollection<S> read(Source<S> source) {
    return new InputCollection<S>(source, this);
  }

  public <K, V> PTable<K, V> read(TableSource<K, V> source) {
    return new InputTable<K, V>(source, this);
  }

  public PCollection<String> readTextFile(String pathName) {
    return read(new TextFileSourceTarget(new Path(pathName)));
  }

  public void write(PCollection<?> pcollection, Target target) {
    if (pcollection instanceof PGroupedTableImpl) {
      pcollection = ((PGroupedTableImpl) pcollection).ungroup();
    }
    outputTargets.put((PCollectionImpl) pcollection, target);
  }

  @Override
  public <T> Iterable<T> materialize(PCollection<T> pcollection) {
	ReadableSourceTarget<T> srcTarget = null;
	if (outputTargets.containsKey(pcollection)) {
	  Target target = outputTargets.get(pcollection);
	  if (target instanceof ReadableSourceTarget) {
		srcTarget = (ReadableSourceTarget) target;
	  }
	}
	if (srcTarget == null) {
	  SourceTarget<T> st = createIntermediateOutput(pcollection.getPType());
	  if (!(srcTarget instanceof ReadableSourceTarget)) {
		throw new IllegalArgumentException("The PType for the given PCollection is not readable"
		    + " and cannot be materialized");
	  } else {
		srcTarget = (ReadableSourceTarget) st;
	  }
	}
	MaterializableIterable<T> c = new MaterializableIterable<T>(this, srcTarget);
	PCollectionImpl impl = (PCollectionImpl) pcollection;
	outputTargets.put(impl, srcTarget);
	outputTargetsToMaterialize.put(impl, c);
	return c;
  }

  public <T> SourceTarget<T> createIntermediateOutput(PType<T> ptype) {
	return ptype.getDefaultFileSource(createTempPath());
  }

  public Path createTempPath() {
    return new Path(tempDirectory, "p" + tempFileIndex++);
  }
  
  @Override
  public <T> void writeTextFile(PCollection<T> pcollection, String pathName) {
    // Ensure that this is a writable pcollection instance.
    pcollection = pcollection.parallelDo("asText", IdentityFn.<T>getInstance(),
        WritableTypeFamily.getInstance().as(pcollection.getPType()));
    write(pcollection, new TextFileSourceTarget(new Path(pathName)));
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

  @Override
  public Configuration getConfiguration() {
    return conf;
  }
  
  public int getNextAnonymousStageId() {
    return nextAnonymousStageId++;
  }
}
