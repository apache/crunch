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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class RecordDropIT {
  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testMultiReadCount() throws Exception {
    int numReads = 10;
    MRPipeline p = new MRPipeline(RecordDropIT.class, tmpDir.getDefaultConfiguration());
    Path shakes = tmpDir.copyResourcePath("shakes.txt");
    TableSource<LongWritable, Text> src = From.formattedFile(shakes,
        TextInputFormat.class, LongWritable.class, Text.class);
    PTable<LongWritable, Text> in = p.read(src);
    List<Iterable<Integer>> values = Lists.newArrayList();
    for (int i = 0; i < numReads; i++) {
      PCollection<Integer> cnt = in.parallelDo(new LineCountFn<Pair<LongWritable, Text>>(), Writables.ints());
      values.add(cnt.materialize());
    }
    int index = 0;
    for (Iterable<Integer> iter : values) {
      assertEquals("Checking index = " + index, 3667, Iterables.getFirst(iter, 0).intValue());
      index++;
    }
    p.done();
  }

  public static class LineCountFn<T> extends DoFn<T, Integer> {

    private int count = 0;

    @Override
    public void process(T input, Emitter<Integer> emitter) {
      count++;
    }

    @Override
    public void cleanup(Emitter<Integer> emitter) {
      emitter.emit(count);
    }
  }
}
