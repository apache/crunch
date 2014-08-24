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
package org.apache.crunch;

import org.apache.crunch.impl.spark.SparkPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.seq.SeqFileTableSourceTarget;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SkipPTypesIT {
  @Rule
  public TemporaryPath tempDir = new TemporaryPath();

  PTableType<Text, LongWritable> ptt = Writables.tableOf(Writables.writables(Text.class),
      Writables.writables(LongWritable.class));

  @Test
  public void testSkipPTypes() throws Exception {
    String out = tempDir.getFileName("out");
    SparkPipeline pipeline = new SparkPipeline("local", "skipptypes");
    PCollection<String> shakes = pipeline.read(From.textFile(tempDir.copyResourceFileName("shakes.txt")));
    PTable<String, Long> wcnt = shakes.count();
    wcnt.write(new MySeqFileTableSourceTarget(out, ptt));
    pipeline.run();

    PTable<Text, LongWritable> wcntIn = pipeline.read(new MySeqFileTableSourceTarget(out, ptt));
    assertEquals(new LongWritable(1L), wcntIn.materialize().iterator().next().second());
    pipeline.done();
  }

  static class ToWritables extends MapFn<Pair<String, Long>, Pair<Text, LongWritable>> {
    @Override
    public Pair<Text, LongWritable> map(Pair<String, Long> input) {
      return Pair.of(new Text(input.first()), new LongWritable(input.second()));
    }
  }
  static class MySeqFileTableSourceTarget extends SeqFileTableSourceTarget {

    public MySeqFileTableSourceTarget(String path, PTableType ptype) {
      super(path, ptype);
    }

    @Override
    public Converter getConverter() {
      return new SkipPTypesConverter(getType().getConverter());
    }
  }

  static class SkipPTypesConverter implements Converter {

    private Converter delegate;

    public SkipPTypesConverter(Converter delegate) {
      this.delegate = delegate;
    }

    @Override
    public Object convertInput(Object key, Object value) {
      return delegate.convertInput(key, value);
    }

    @Override
    public Object convertIterableInput(Object key, Iterable value) {
      return delegate.convertIterableInput(key, value);
    }

    @Override
    public Object outputKey(Object value) {
      return delegate.outputKey(value);
    }

    @Override
    public Object outputValue(Object value) {
      return delegate.outputValue(value);
    }

    @Override
    public Class getKeyClass() {
      return delegate.getKeyClass();
    }

    @Override
    public Class getValueClass() {
      return delegate.getValueClass();
    }

    @Override
    public boolean applyPTypeTransforms() {
      return false;
    }
  }
}
