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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.crunch.CachingOptions;
import org.apache.crunch.CreateOptions;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.PipelineExecution;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.dist.DistributedPipeline;
import org.apache.crunch.impl.dist.collect.PCollectionImpl;
import org.apache.crunch.impl.spark.collect.CreatedCollection;
import org.apache.crunch.impl.spark.collect.CreatedTable;
import org.apache.crunch.impl.spark.collect.EmptyPCollection;
import org.apache.crunch.impl.spark.collect.EmptyPTable;
import org.apache.crunch.impl.spark.collect.SparkCollectFactory;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.materialize.MaterializableIterable;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SparkPipeline extends DistributedPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(SparkPipeline.class);

  private final String sparkConnect;
  private JavaSparkContext sparkContext;
  private Class<?> jarClass;
  private final Map<PCollection<?>, StorageLevel> cachedCollections = Maps.newHashMap();

  public SparkPipeline(String sparkConnect, String appName) {
    this(sparkConnect, appName, null);
  }

  public SparkPipeline(String sparkConnect, String appName, Class<?> jarClass) {
    this(sparkConnect, appName, jarClass, new Configuration());
  }

  public SparkPipeline(String sparkConnect, String appName, Class<?> jarClass, Configuration conf) {
    super(appName, conf, new SparkCollectFactory());
    this.sparkConnect = Preconditions.checkNotNull(sparkConnect);
    this.jarClass = jarClass;
  }

  public SparkPipeline(JavaSparkContext sparkContext, String appName) {
    this(sparkContext, appName, null, sparkContext != null? sparkContext.hadoopConfiguration(): new Configuration());
  }

  public SparkPipeline(JavaSparkContext sparkContext, String appName, Class<?> jarClass, Configuration conf) {
    super(appName, conf, new SparkCollectFactory());
    this.sparkContext = Preconditions.checkNotNull(sparkContext);
    this.sparkConnect = sparkContext.getSparkHome().orNull();
    this.jarClass = jarClass;
  }

  @Override
  public <T> Iterable<T> materialize(PCollection<T> pcollection) {
    ReadableSource<T> readableSrc = getMaterializeSourceTarget(pcollection);
    MaterializableIterable<T> c = new MaterializableIterable<T>(this, readableSrc);
    if (!outputTargetsToMaterialize.containsKey(pcollection)) {
      outputTargetsToMaterialize.put((PCollectionImpl) pcollection, c);
    }
    return c;
  }

  @Override
  public <S> PCollection<S> emptyPCollection(PType<S> ptype) {
    return new EmptyPCollection<S>(this, ptype);
  }

  @Override
  public <K, V> PTable<K, V> emptyPTable(PTableType<K, V> ptype) {
    return new EmptyPTable<K, V>(this, ptype);
  }

  @Override
  public <S> PCollection<S> create(Iterable<S> contents, PType<S> ptype, CreateOptions options) {
    return new CreatedCollection<S>(this, contents, ptype, options);
  }

  @Override
  public <K, V> PTable<K, V> create(Iterable<Pair<K, V>> contents, PTableType<K, V> ptype, CreateOptions options) {
    return new CreatedTable<K, V>(this, contents, ptype, options);
  }

  @Override
  public <T> void cache(PCollection<T> pcollection, CachingOptions options) {
    cachedCollections.put(pcollection, toStorageLevel(options));
  }

  private StorageLevel toStorageLevel(CachingOptions options) {
    return StorageLevel.apply(
        options.useDisk(),
        options.useMemory(),
        options.deserialized(),
        options.replicas());
  }

  @Override
  public PipelineResult run() {
    try {
      PipelineExecution exec = runAsync();
      exec.waitUntilDone();
      return exec.getResult();
    } catch (Exception e) {
      LOG.error("Exception running pipeline", e);
      return PipelineResult.EMPTY;
    }
  }

  @Override
  public PipelineExecution runAsync() {
    Map<PCollectionImpl<?>, MaterializableIterable> toMaterialize = Maps.newHashMap();
    for (PCollectionImpl<?> c : outputTargets.keySet()) {
      if (outputTargetsToMaterialize.containsKey(c)) {
        toMaterialize.put(c, outputTargetsToMaterialize.get(c));
        outputTargetsToMaterialize.remove(c);
      }
    }

    Configuration conf = getConfiguration();
    if (sparkContext == null) {
      SparkConf sparkConf = new SparkConf();
      for (Map.Entry<String, String> e : conf) {
        if (e.getKey().startsWith("spark.")) {
          sparkConf.set(e.getKey(), e.getValue());
        }
      }
      this.sparkContext = new JavaSparkContext(sparkConnect, getName(), sparkConf);
      if (jarClass != null) {
        String[] jars = JavaSparkContext.jarOfClass(jarClass);
        if (jars != null && jars.length > 0) {
          for (String jar : jars) {
            sparkContext.addJar(jar);
          }
        }
      }
    }

    copyConfiguration(conf, sparkContext.hadoopConfiguration());
    SparkRuntime runtime = new SparkRuntime(this, sparkContext, conf, outputTargets,
        toMaterialize, cachedCollections, allPipelineCallables);
    runtime.execute();
    outputTargets.clear();
    return runtime;
  }

  @Override
  public PipelineResult done() {
    PipelineResult res = super.done();
    if (sparkContext != null) {
      sparkContext.stop();
      sparkContext = null;
    }
    return res;
  }

  private static void copyConfiguration(Configuration from, Configuration to) {
    for (Map.Entry<String, String> e : from) {
      to.set(e.getKey(), e.getValue());
    }
  }
}
