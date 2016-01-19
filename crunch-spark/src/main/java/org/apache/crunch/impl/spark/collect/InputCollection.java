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

import org.apache.crunch.MapFn;
import org.apache.crunch.ParallelDoOptions;
import org.apache.crunch.Source;
import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.impl.dist.DistributedPipeline;
import org.apache.crunch.impl.dist.collect.BaseInputCollection;
import org.apache.crunch.impl.mr.run.CrunchInputFormat;
import org.apache.crunch.impl.spark.SparkCollection;
import org.apache.crunch.impl.spark.SparkRuntime;
import org.apache.crunch.impl.spark.fn.InputConverterFunction;
import org.apache.crunch.impl.spark.fn.MapFunction;
import org.apache.crunch.types.Converter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;

import java.io.IOException;

public class InputCollection<S> extends BaseInputCollection<S> implements SparkCollection {

  InputCollection(Source<S> source, String named, DistributedPipeline pipeline, ParallelDoOptions doOpts) {
    super(source, named, pipeline, doOpts);
  }

  public JavaRDDLike<?, ?> getJavaRDDLike(SparkRuntime runtime) {
    try {
      Job job = new Job(runtime.getConfiguration());
      FileInputFormat.addInputPaths(job, "/tmp"); //placeholder
      source.configureSource(job, 0);
      Converter converter = source.getConverter();
      JavaPairRDD<?, ?> input = runtime.getSparkContext().newAPIHadoopRDD(
          job.getConfiguration(),
          CrunchInputFormat.class,
          converter.getKeyClass(),
          converter.getValueClass());
      input.rdd().setName(getName());
      MapFn mapFn = converter.applyPTypeTransforms() ? source.getType().getInputMapFn() : IdentityFn.getInstance();
      return input
          .map(new InputConverterFunction(source.getConverter()))
          .map(new MapFunction(mapFn, runtime.getRuntimeContext()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
