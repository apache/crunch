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
import org.apache.crunch.Pair;
import org.apache.crunch.ReadableData;
import org.apache.crunch.impl.dist.collect.PCollectionImpl;
import org.apache.crunch.impl.dist.collect.PTableBase;
import org.apache.crunch.impl.spark.ByteArray;
import org.apache.crunch.impl.spark.SparkCollection;
import org.apache.crunch.impl.spark.SparkPipeline;
import org.apache.crunch.impl.spark.SparkRuntime;
import org.apache.crunch.impl.spark.SparkRuntimeContext;
import org.apache.crunch.impl.spark.serde.SerDe;
import org.apache.crunch.impl.spark.serde.SerDeFactory;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;

/**
 * Represents a Spark-based PTable that was created from a Java {@code Iterable} of
 * key-value pairs.
 */
public class CreatedTable<K, V> extends PTableBase<K, V> implements SparkCollection {

  private final Iterable<Pair<K, V>> contents;
  private final PTableType<K, V> ptype;
  private final int parallelism;
  private JavaPairRDD<K, V> rdd;

  public CreatedTable(
      SparkPipeline pipeline,
      Iterable<Pair<K, V>> contents,
      PTableType<K, V> ptype,
      CreateOptions options) {
    super(options.getName(), pipeline);
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
  protected ReadableData<Pair<K, V>> getReadableDataInternal() {
    try {
      return ptype.createSourceTarget(pipeline.getConfiguration(),
              pipeline.createTempPath(), contents, parallelism).asReadable();
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
  public PTableType<K, V> getPTableType() {
    return ptype;
  }

  @Override
  public PType<Pair<K, V>> getPType() {
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

  private JavaPairRDD<K, V> getJavaRDDLikeInternal(SparkRuntime runtime) {
    ptype.initialize(runtime.getConfiguration());
    PType keyType = ptype.getKeyType();
    PType valueType = ptype.getValueType();
    SerDe keySerde = SerDeFactory.create(keyType, runtime.getConfiguration());
    SerDe valueSerde = SerDeFactory.create(valueType, runtime.getConfiguration());
    List<Tuple2<ByteArray, ByteArray>> res = Lists.newLinkedList();
    try {
      for (Pair<K, V> p : contents) {
        ByteArray key = keySerde.toBytes(keyType.getOutputMapFn().map(p.first()));
        ByteArray value = valueSerde.toBytes(valueType.getOutputMapFn().map(p.second()));
        res.add(new Tuple2<ByteArray, ByteArray>(key, value));
      }
    } catch (Exception e) {
      throw new CrunchRuntimeException(e);
    }
    return runtime.getSparkContext()
        .parallelizePairs(res, parallelism)
        .mapToPair(new MapPairInputFn<K, V>(
            keySerde, valueSerde, keyType.getInputMapFn(), valueType.getInputMapFn(), runtime.getRuntimeContext()));
  }

  static class MapPairInputFn<K, V> implements PairFunction<Tuple2<ByteArray, ByteArray>, K, V> {

    private final SerDe keySerde;
    private final SerDe valueSerde;
    private final MapFn<Object, K> keyFn;
    private final MapFn<Object, V> valueFn;
    private final SparkRuntimeContext context;
    private boolean initialized;

    public MapPairInputFn(
        SerDe keySerde,
        SerDe valueSerde,
        MapFn<Object, K> keyFn,
        MapFn<Object, V> valueFn,
        SparkRuntimeContext context) {
      this.keySerde = keySerde;
      this.valueSerde = valueSerde;
      this.keyFn = keyFn;
      this.valueFn = valueFn;
      this.context = context;
      this.initialized = false;
    }

    @Override
    public Tuple2<K, V> call(Tuple2<ByteArray, ByteArray> in) throws Exception {
      if (!initialized) {
        context.initialize(keyFn, -1);
        context.initialize(valueFn, -1);
        initialized = true;
      }
      return new Tuple2<K, V>(
          keyFn.map(keySerde.fromBytes(in._1().value)),
              valueFn.map(valueSerde.fromBytes(in._2().value)));
    }
  }
}
