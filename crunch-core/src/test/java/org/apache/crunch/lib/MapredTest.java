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

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.crunch.MapFn;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.Writables;

import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Before;
import org.junit.Test;

public class MapredTest implements Serializable {

  private static class TestMapper implements Mapper<IntWritable, Text, Text, LongWritable> {
    @Override
    public void configure(JobConf arg0) {
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void map(IntWritable k, Text v, OutputCollector<Text, LongWritable> out,
        Reporter reporter) throws IOException {
      reporter.getCounter("written", "out").increment(1L);
      out.collect(new Text(v), new LongWritable(v.getLength()));
    }
  }
  
  private static class TestReducer implements Reducer<IntWritable, Text, Text, LongWritable> {

    @Override
    public void configure(JobConf arg0) { 
    }

    @Override
    public void close() throws IOException { 
    }

    @Override
    public void reduce(IntWritable key, Iterator<Text> iter,
        OutputCollector<Text, LongWritable> out, Reporter reporter) throws IOException {
      boolean hasBall = false;
      String notBall = "";
      while (iter.hasNext()) {
        String next = iter.next().toString();
        if ("ball".equals(next)) {
          reporter.getCounter("foo", "bar").increment(1);
          hasBall = true;
        } else {
          notBall = next;
        }
      }
      out.collect(new Text(notBall), hasBall ? new LongWritable(1L) : new LongWritable(0L));
    }
  }
  
  private static Pair<Text, LongWritable> $(String one, int two) {
    return Pair.of(new Text(one), new LongWritable(two));
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
    
    PTable<Text, LongWritable> out = Mapred.map(two, TestMapper.class, Text.class, LongWritable.class);
    assertEquals(ImmutableList.of($("foot", 4), $("ball", 4), $("bazzar", 6)),
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
    PTable<Text, LongWritable> out = Mapred.reduce(two.groupByKey(), TestReducer.class, Text.class, LongWritable.class);
    assertEquals(ImmutableList.of($("foot", 1), $("base", 1), $("basket", 1), $("hockey", 0)),
        Lists.newArrayList(out.materialize()));
    assertEquals(3, MemPipeline.getCounters().findCounter("foo", "bar").getValue());
  }
}
