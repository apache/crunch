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
package org.apache.crunch.lib;

import static org.junit.Assert.assertEquals;

import org.apache.crunch.MapFn;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class MapreduceTest {
  private static class TestMapper extends Mapper<IntWritable, Text, IntWritable, Text> {
    @Override
    protected void map(IntWritable k, Text v, Mapper<IntWritable, Text, IntWritable, Text>.Context ctxt) {
      try {
        ctxt.write(new IntWritable(v.getLength()), v);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  private static class TestReducer extends Reducer<IntWritable, Text, Text, LongWritable> {
    protected void reduce(IntWritable key, Iterable<Text> values,
        org.apache.hadoop.mapreduce.Reducer<IntWritable, Text, Text, LongWritable>.Context ctxt) {
      boolean hasBall = false;
      String notBall = "";
      for (Text t : values) {
        String next = t.toString();
        if ("ball".equals(next)) {
          hasBall = true;
          ctxt.getCounter("foo", "bar").increment(1);
        } else {
          notBall = next;
        }
      }
      try {
        ctxt.write(new Text(notBall), hasBall ? new LongWritable(1L) : new LongWritable(0L));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  private static Pair<Text, LongWritable> $1(String one, int two) {
    return Pair.of(new Text(one), new LongWritable(two));
  }
  
  private static Pair<IntWritable, Text> $2(int one, String two) {
    return Pair.of(new IntWritable(one), new Text(two));
  }
  
  @Before
  public void setUp() {
    MemPipeline.clearCounters();
  }
  
  @Test
  public void testMapper() throws Exception {
    PTable<Integer, String> in = MemPipeline.typedTableOf(Avros.tableOf(Avros.ints(), Avros.strings()),
        1, "foot", 2, "ball", 3, "bazzar");
    PTable<IntWritable, Text> two = in.parallelDo(new MapFn<Pair<Integer, String>, Pair<IntWritable, Text>>() {
      @Override
      public Pair<IntWritable, Text> map(Pair<Integer, String> input) {
        return Pair.of(new IntWritable(input.first()), new Text(input.second()));
      }
    }, Writables.tableOf(Writables.writables(IntWritable.class), Writables.writables(Text.class)));
    
    PTable<IntWritable, Text> out = Mapreduce.map(two, TestMapper.class, IntWritable.class, Text.class);
    assertEquals(ImmutableList.of($2(4, "foot"), $2(4, "ball"), $2(6, "bazzar")),
        Lists.newArrayList(out.materialize()));
  }
  
  @Test
  public void testReducer() throws Exception {
    PTable<Integer, String> in = MemPipeline.typedTableOf(Avros.tableOf(Avros.ints(), Avros.strings()),
        1, "foot", 1, "ball", 2, "base", 2, "ball", 3, "basket", 3, "ball", 4, "hockey");
    PTable<IntWritable, Text> two = in.parallelDo(new MapFn<Pair<Integer, String>, Pair<IntWritable, Text>>() {
      @Override
      public Pair<IntWritable, Text> map(Pair<Integer, String> input) {
        return Pair.of(new IntWritable(input.first()), new Text(input.second()));
      }
    }, Writables.tableOf(Writables.writables(IntWritable.class), Writables.writables(Text.class)));
    PTable<Text, LongWritable> out = Mapreduce.reduce(two.groupByKey(), TestReducer.class, Text.class, LongWritable.class);
    assertEquals(ImmutableList.of($1("foot", 1), $1("base", 1), $1("basket", 1), $1("hockey", 0)),
        Lists.newArrayList(out.materialize()));
    assertEquals(3, MemPipeline.getCounters().findCounter("foo", "bar").getValue());
  }
}
