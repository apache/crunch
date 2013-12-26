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
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.PipelineExecution;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.dist.DistributedPipeline;
import org.apache.crunch.impl.dist.collect.PCollectionImpl;
import org.apache.crunch.impl.spark.collect.EmptyPCollection;
import org.apache.crunch.impl.spark.collect.EmptyPTable;
import org.apache.crunch.impl.spark.collect.SparkCollectFactory;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.materialize.MaterializableIterable;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Map;

public class SparkPipeline extends DistributedPipeline {

  private final String sparkConnect;
  private JavaSparkContext sparkContext;
  private final Map<PCollection<?>, StorageLevel> cachedCollections = Maps.newHashMap();

  public SparkPipeline(String sparkConnect, String appName) {
    super(appName, new Configuration(), new SparkCollectFactory());
    this.sparkConnect = Preconditions.checkNotNull(sparkConnect);
  }

  public SparkPipeline(JavaSparkContext sparkContext, String appName) {
    super(appName, new Configuration(), new SparkCollectFactory());
    this.sparkContext = Preconditions.checkNotNull(sparkContext);
    this.sparkConnect = sparkContext.getSparkHome().orNull();
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
      // TODO: How to handle this without changing signature?
      // LOG.error("Exception running pipeline", e);
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
    if (sparkContext == null) {
      this.sparkContext = new JavaSparkContext(sparkConnect, getName());
    }
    SparkRuntime runtime = new SparkRuntime(this, sparkContext, getConfiguration(), outputTargets, toMaterialize,
        cachedCollections);
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
}
