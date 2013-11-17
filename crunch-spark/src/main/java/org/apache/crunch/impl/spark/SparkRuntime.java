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
package org.apache.crunch.impl.spark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractFuture;
import org.apache.crunch.CombineFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PipelineExecution;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.Target;
import org.apache.crunch.impl.dist.collect.PCollectionImpl;
import org.apache.crunch.impl.spark.fn.MapFunction;
import org.apache.crunch.impl.spark.fn.OutputConverterFunction;
import org.apache.crunch.impl.spark.fn.PairMapFunction;
import org.apache.crunch.io.MapReduceTarget;
import org.apache.crunch.io.PathTarget;
import org.apache.crunch.materialize.MaterializableIterable;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.io.IOException;
import java.net.URI;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class SparkRuntime extends AbstractFuture<PipelineResult> implements PipelineExecution {

  private SparkPipeline pipeline;
  private JavaSparkContext sparkContext;
  private Configuration conf;
  private CombineFn combineFn;
  private SparkRuntimeContext ctxt;
  private Map<PCollectionImpl<?>, Set<Target>> outputTargets;
  private Map<PCollectionImpl<?>, MaterializableIterable> toMaterialize;
  private Map<PCollection<?>, StorageLevel> toCache;
  private final CountDownLatch doneSignal = new CountDownLatch(1);
  private AtomicReference<Status> status = new AtomicReference<Status>(Status.READY);
  private PipelineResult result;
  private boolean started;
  private Thread monitorThread;

  // Note that this is the oppposite of the MR sort
  static final Comparator<PCollectionImpl<?>> DEPTH_COMPARATOR = new Comparator<PCollectionImpl<?>>() {
    @Override
    public int compare(PCollectionImpl<?> left, PCollectionImpl<?> right) {
      int cmp = left.getDepth() - right.getDepth();
      if (cmp == 0) {
        // Ensure we don't throw away two output collections at the same depth.
        // Using the collection name would be nicer here, but names aren't
        // necessarily unique.
        cmp = new Integer(left.hashCode()).compareTo(right.hashCode());
      }
      return cmp;
    }
  };

  public SparkRuntime(SparkPipeline pipeline,
                      JavaSparkContext sparkContext,
                      Configuration conf,
                      Map<PCollectionImpl<?>, Set<Target>> outputTargets,
                      Map<PCollectionImpl<?>, MaterializableIterable> toMaterialize,
                      Map<PCollection<?>, StorageLevel> toCache) {
    this.pipeline = pipeline;
    this.sparkContext = sparkContext;
    this.conf = conf;
    this.ctxt = new SparkRuntimeContext(
        sparkContext.broadcast(conf),
        sparkContext.accumulator(Maps.<String, Long>newHashMap(), new CounterAccumulatorParam()));
    this.outputTargets = Maps.newTreeMap(DEPTH_COMPARATOR);
    this.outputTargets.putAll(outputTargets);
    this.toMaterialize = toMaterialize;
    this.toCache = toCache;
    this.status.set(Status.READY);
    this.monitorThread = new Thread(new Runnable() {
      @Override
      public void run() {
        monitorLoop();
      }
    });
  }

  public void setCombineFn(CombineFn combineFn) {
    this.combineFn = combineFn;
  }

  public CombineFn getCombineFn() {
    CombineFn ret = combineFn;
    this.combineFn = null;
    return ret;
  }

  private void distributeFiles() {
    try {
      URI[] uris = DistributedCache.getCacheFiles(conf);
      if (uris != null) {
        URI[] outURIs = new URI[uris.length];
        for (int i = 0; i < uris.length; i++) {
          Path path = new Path(uris[i]);
          FileSystem fs = path.getFileSystem(conf);
          if (fs.isFile(path)) {
            outURIs[i] = uris[i];
          } else {
            Path mergePath = new Path(path.getParent(), "sparkreadable-" + path.getName());
            FileUtil.copyMerge(fs, path, fs, mergePath, false, conf, "");
            outURIs[i] = mergePath.toUri();
          }
          sparkContext.addFile(outURIs[i].toString());
        }
        DistributedCache.setCacheFiles(outURIs, conf);
      }
    } catch (IOException e) {
      throw new RuntimeException("Error retrieving cache files", e);
    }
  }

  public synchronized SparkRuntime execute() {
    if (!started) {
      monitorThread.start();
      started = true;
    }
    return this;
  }

  public JavaSparkContext getSparkContext() {
    return sparkContext;
  }

  public SparkRuntimeContext getRuntimeContext() {
    return ctxt;
  }

  public Configuration getConfiguration() {
    return conf;
  }

  public boolean isValid(JavaRDDLike<?, ?> rdd) {
    return (rdd != null); //TODO: support multi-contexts
  }

  public StorageLevel getStorageLevel(PCollection<?> pcollection) {
    return toCache.get(pcollection);
  }

  @Override
  public String getPlanDotFile() {
    return "";
  }

  @Override
  public void waitFor(long timeout, TimeUnit timeUnit) throws InterruptedException {
    doneSignal.await(timeout, timeUnit);
  }

  @Override
  public void waitUntilDone() throws InterruptedException {
    doneSignal.await();
  }

  private void monitorLoop() {
    status.set(Status.RUNNING);
    Map<PCollectionImpl<?>, Set<SourceTarget<?>>> targetDeps = Maps.<PCollectionImpl<?>, PCollectionImpl<?>, Set<SourceTarget<?>>>newTreeMap(DEPTH_COMPARATOR);
    for (PCollectionImpl<?> pcollect : outputTargets.keySet()) {
      targetDeps.put(pcollect, pcollect.getTargetDependencies());
    }

    while (!targetDeps.isEmpty() && doneSignal.getCount() > 0) {
      Set<Target> allTargets = Sets.newHashSet();
      for (PCollectionImpl<?> pcollect : targetDeps.keySet()) {
        allTargets.addAll(outputTargets.get(pcollect));
      }

      Map<PCollectionImpl<?>, JavaRDDLike<?, ?>> pcolToRdd = Maps.newTreeMap(DEPTH_COMPARATOR);
      for (PCollectionImpl<?> pcollect : targetDeps.keySet()) {
        if (Sets.intersection(allTargets, targetDeps.get(pcollect)).isEmpty()) {
          JavaRDDLike<?, ?> rdd = ((SparkCollection) pcollect).getJavaRDDLike(this);
          pcolToRdd.put(pcollect, rdd);
        }
      }
      distributeFiles();
      for (Map.Entry<PCollectionImpl<?>, JavaRDDLike<?, ?>> e : pcolToRdd.entrySet()) {
        JavaRDDLike<?, ?> rdd = e.getValue();
        PType<?> ptype = e.getKey().getPType();
        Set<Target> targets = outputTargets.get(e.getKey());
        if (targets.size() > 1) {
          rdd.rdd().cache();
        }
        for (Target t : targets) {
          Configuration conf = new Configuration(getConfiguration());
          if (t instanceof MapReduceTarget) { //TODO: check this earlier
            Converter c = t.getConverter(ptype);
            JavaPairRDD<?, ?> outRDD;
            if (rdd instanceof JavaRDD) {
              outRDD = ((JavaRDD) rdd)
                  .map(new MapFunction(ptype.getOutputMapFn(), ctxt))
                  .map(new OutputConverterFunction(c));
            } else {
              outRDD = ((JavaPairRDD) rdd)
                  .map(new PairMapFunction(ptype.getOutputMapFn(), ctxt))
                  .map(new OutputConverterFunction(c));
            }

            try {
              Job job = new Job(conf);
              if (t instanceof PathTarget) {
                PathTarget pt = (PathTarget) t;
                pt.configureForMapReduce(job, ptype, pt.getPath(), null);
                Path tmpPath = pipeline.createTempPath();
                outRDD.saveAsNewAPIHadoopFile(
                    tmpPath.toString(),
                    c.getKeyClass(),
                    c.getValueClass(),
                    job.getOutputFormatClass(),
                    job.getConfiguration());
                pt.handleOutputs(job.getConfiguration(), tmpPath, -1);
              } else if (t instanceof MapReduceTarget) {
                MapReduceTarget mrt = (MapReduceTarget) t;
                mrt.configureForMapReduce(job, ptype, new Path("/tmp"), null);
                outRDD.saveAsHadoopDataset(new JobConf(job.getConfiguration()));
              } else {
                throw new IllegalArgumentException("Spark execution cannot handle non-MapReduceTarget: " + t);
              }
            } catch (Exception et) {
              et.printStackTrace();
              status.set(Status.FAILED);
              set(PipelineResult.EMPTY);
              doneSignal.countDown();
            }
          }
        }
      }
      for (PCollectionImpl<?> output : pcolToRdd.keySet()) {
        if (toMaterialize.containsKey(output)) {
          MaterializableIterable mi = toMaterialize.get(output);
          if (mi.isSourceTarget()) {
            output.materializeAt((SourceTarget) mi.getSource());
          }
        }
        targetDeps.remove(output);
      }
    }
    if (status.get() != Status.FAILED || status.get() != Status.KILLED) {
      status.set(Status.SUCCEEDED);
      result = new PipelineResult(ImmutableList.of(new PipelineResult.StageResult("Spark", null)),
          Status.SUCCEEDED);
      set(result);
    } else {
      set(PipelineResult.EMPTY);
    }
    doneSignal.countDown();
  }

  @Override
  public PipelineResult get() throws InterruptedException, ExecutionException {
    if (getStatus() == Status.READY) {
      execute();
    }
    return super.get();
  }

  @Override
  public PipelineResult get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException,
      ExecutionException {
    if (getStatus() == Status.READY) {
      execute();
    }
    return super.get(timeout, unit);
  }

  @Override
  public Status getStatus() {
    return status.get();
  }

  @Override
  public PipelineResult getResult() {
    return result;
  }

  @Override
  public void kill() throws InterruptedException {
    if (started) {
      sparkContext.stop();
      set(PipelineResult.EMPTY);
    }
  }
}
