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
package org.apache.crunch.impl.spark.collect;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.crunch.CreateOptions;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.MapFn;
import org.apache.crunch.ReadableData;
import org.apache.crunch.impl.dist.collect.PCollectionImpl;
import org.apache.crunch.impl.spark.ByteArray;
import org.apache.crunch.impl.spark.SparkCollection;
import org.apache.crunch.impl.spark.SparkPipeline;
import org.apache.crunch.impl.spark.SparkRuntime;
import org.apache.crunch.impl.spark.SparkRuntimeContext;
import org.apache.crunch.impl.spark.serde.SerDe;
import org.apache.crunch.impl.spark.serde.SerDeFactory;
import org.apache.crunch.types.PType;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

import java.io.IOException;
import java.util.List;

/**
 * Represents a Spark-based PCollection that was created from a Java {@code Iterable} of
 * values.
 */
public class CreatedCollection<T> extends PCollectionImpl<T> implements SparkCollection {

  private final Iterable<T> contents;
  private final PType<T> ptype;
  private final int parallelism;
  private JavaRDD<T> rdd;

  public CreatedCollection(SparkPipeline p, Iterable<T> contents, PType<T> ptype, CreateOptions options) {
    super(options.getName(), p);
    this.contents = contents;
    this.ptype = ptype;
    this.parallelism = options.getParallelism();
  }

  @Override
  protected void acceptInternal(Visitor visitor) {
    // No-op
  }

  @Override
  public List<PCollectionImpl<?>> getParents() {
    return ImmutableList.of();
  }

  @Override
  protected ReadableData<T> getReadableDataInternal() {
    try {
      return ptype.createSourceTarget(getPipeline().getConfiguration(),
              getPipeline().createTempPath(), contents, parallelism).asReadable();
    } catch (IOException e) {
      throw new CrunchRuntimeException(e);
    }
  }

  @Override
  protected long getSizeInternal() {
    return Iterables.size(contents);
  }

  @Override
  public long getLastModifiedAt() {
    return -1;
  }

  @Override
  public PType<T> getPType() {
    return ptype;
  }

  @Override
  public JavaRDDLike<?, ?> getJavaRDDLike(SparkRuntime runtime) {
    if (!runtime.isValid(rdd)) {
      rdd = getJavaRDDLikeInternal(runtime);
      rdd.rdd().setName(getName());
      StorageLevel sl = runtime.getStorageLevel(this);
      if (sl != null) {
        rdd.rdd().persist(sl);
      }
    }
    return rdd;
  }

  private JavaRDD<T> getJavaRDDLikeInternal(SparkRuntime runtime) {
    SerDe serde = SerDeFactory.create(ptype, runtime.getConfiguration());
    ptype.initialize(runtime.getConfiguration());
    List<ByteArray> res = Lists.newLinkedList();
    try {
      for (T value : contents) {
        res.add(serde.toBytes(ptype.getOutputMapFn().map(value)));
      }
    } catch (Exception e) {
      throw new CrunchRuntimeException(e);
    }
    return runtime.getSparkContext()
        .parallelize(res, parallelism)
        .map(new MapInputFn<T>(serde, ptype.getInputMapFn(), runtime.getRuntimeContext()));
  }

  static class MapInputFn<T> implements Function<ByteArray, T> {

    private final SerDe serde;
    private final MapFn<Object, T> fn;
    private final SparkRuntimeContext context;
    private boolean initialized;

    public MapInputFn(SerDe serde, MapFn<Object, T> fn, SparkRuntimeContext context) {
      this.serde = serde;
      this.fn = fn;
      this.context = context;
      this.initialized = false;
    }

    @Override
    public T call(ByteArray byteArray) throws Exception {
      if (!initialized) {
        context.initialize(fn, -1);
        initialized = true;
      }
      return fn.map(serde.fromBytes(byteArray.value));
    }
  }
}
