/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.crunch.io.avro;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.Aggregator;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.Source;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.avro.AvroMode;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroModeIT implements Serializable {

  public static final Schema GENERIC_SCHEMA = new Schema.Parser().parse("{\n" +
      "  \"name\": \"mystring\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "    { \"name\": \"text\", \"type\": \"string\" }\n" +
      "  ]\n" +
      "}");

  static final class FloatArray {
    private final float[] values;
    FloatArray() {
      this(null);
    }
    FloatArray(float[] values) {
      this.values = values;
    }
    float[] getValues() {
      return values;
    }
  }

  public static AvroType<float[]> FLOAT_ARRAY = Avros.derived(float[].class,
      new MapFn<FloatArray, float[]>() {
        @Override
        public float[] map(FloatArray input) {
          return input.getValues();
        }
      },
      new MapFn<float[], FloatArray>() {
        @Override
        public FloatArray map(float[] input) {
          return new FloatArray(input);
        }
      }, Avros.reflects(FloatArray.class));

  @Rule
  public transient TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testGenericReflectConflict() throws IOException {
    final Random rand = new Random();
    rand.setSeed(12345);
    Configuration conf = new Configuration();
    Pipeline pipeline = new MRPipeline(AvroModeIT.class, conf);
    Source<GenericData.Record> source = From.avroFile(
        tmpDir.copyResourceFileName("strings-100.avro"),
        Avros.generics(GENERIC_SCHEMA));
    PTable<Long, float[]> mapPhase = pipeline
        .read(source)
        .parallelDo(new DoFn<GenericData.Record, Pair<Long, float[]>>() {
          @Override
          public void process(GenericData.Record input, Emitter<Pair<Long, float[]>> emitter) {
            emitter.emit(Pair.of(
                Long.valueOf(input.get("text").toString().length()),
                new float[] {rand.nextFloat(), rand.nextFloat()}));
          }
        }, Avros.tableOf(Avros.longs(), FLOAT_ARRAY));

    PTable<Long, float[]> result = mapPhase
        .groupByKey()
        .combineValues(new Aggregator<float[]>() {
          float[] accumulator = null;

          @Override
          public Iterable<float[]> results() {
            return ImmutableList.of(accumulator);
          }

          @Override
          public void initialize(Configuration conf) {
          }

          @Override
          public void reset() {
            this.accumulator = null;
          }

          @Override
          public void update(float[] value) {
            if (accumulator == null) {
              accumulator = Arrays.copyOf(value, 2);
            } else {
              for (int i = 0; i < value.length; i += 1) {
                accumulator[i] += value[i];
              }
            }
          }
        });

    pipeline.writeTextFile(result, tmpDir.getFileName("unused"));
    Assert.assertTrue("Should succeed", pipeline.done().succeeded());
  }
}
