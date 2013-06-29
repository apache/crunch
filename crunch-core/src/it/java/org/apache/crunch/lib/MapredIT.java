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

import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.PipelineResult.StageResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.test.CrunchTestSupport;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

public class MapredIT extends CrunchTestSupport implements Serializable {
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
      out.collect(v, new LongWritable(v.getLength()));
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
      boolean hasThou = false;
      String notThou = "";
      while (iter.hasNext()) {
        String next = iter.next().toString();
        if (next != null && next.contains("thou")) {
          reporter.getCounter("thou", "count").increment(1);
          hasThou = true;
        } else {
          notThou = next;
        }
      }
      out.collect(new Text(notThou), hasThou ? new LongWritable(1L) : new LongWritable(0L));
    }
  }
  
  @Test
  public void testMapper() throws Exception {
    Pipeline p = new MRPipeline(MapredIT.class, tempDir.getDefaultConfiguration());
    Path shakesPath = tempDir.copyResourcePath("shakes.txt");
    PCollection<String> in = p.read(From.textFile(shakesPath));
    PTable<IntWritable, Text> two = in.parallelDo(new MapFn<String, Pair<IntWritable, Text>>() {
      @Override
      public Pair<IntWritable, Text> map(String input) {
        return Pair.of(new IntWritable(input.length()), new Text(input));
      }
    }, Writables.tableOf(Writables.writables(IntWritable.class), Writables.writables(Text.class)));
    
    PTable<Text, LongWritable> out = Mapred.map(two, TestMapper.class, Text.class, LongWritable.class);
    out.write(To.sequenceFile(tempDir.getPath("temp")));
    PipelineResult res = p.done();
    assertEquals(1, res.getStageResults().size());
    StageResult sr = res.getStageResults().get(0);
    assertEquals(3667, sr.getCounters().findCounter("written", "out").getValue());
  }
  
  @Test
  public void testReducer() throws Exception {
    Pipeline p = new MRPipeline(MapredIT.class, tempDir.getDefaultConfiguration());
    Path shakesPath = tempDir.copyResourcePath("shakes.txt");
    PCollection<String> in = p.read(From.textFile(shakesPath));
    PTable<IntWritable, Text> two = in.parallelDo(new MapFn<String, Pair<IntWritable, Text>>() {
      @Override
      public Pair<IntWritable, Text> map(String input) {
        return Pair.of(new IntWritable(input.length()), new Text(input));
      }
    }, Writables.tableOf(Writables.writables(IntWritable.class), Writables.writables(Text.class)));
    
    PTable<Text, LongWritable> out = Mapred.reduce(two.groupByKey(), TestReducer.class, Text.class, LongWritable.class);
    out.write(To.sequenceFile(tempDir.getPath("temp")));
    PipelineResult res = p.done();
    assertEquals(1, res.getStageResults().size());
    StageResult sr = res.getStageResults().get(0);
    assertEquals(108, sr.getCounters().findCounter("thou", "count").getValue());
  }
}
