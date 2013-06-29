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

import java.io.Serializable;

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
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;

public class MapreduceIT extends CrunchTestSupport implements Serializable {
  private static class TestMapper extends Mapper<IntWritable, Text, IntWritable, Text> {
    @Override
    protected void map(IntWritable k, Text v, Mapper<IntWritable, Text, IntWritable, Text>.Context ctxt) {
      try {
        ctxt.getCounter("written", "out").increment(1L);
        ctxt.write(new IntWritable(v.getLength()), v);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  private static class TestReducer extends Reducer<IntWritable, Text, Text, LongWritable> {
    protected void reduce(IntWritable key, Iterable<Text> values,
        org.apache.hadoop.mapreduce.Reducer<IntWritable, Text, Text, LongWritable>.Context ctxt) {
      boolean hasWhere = false;
      String notWhere = "";
      for (Text t : values) {
        String next = t.toString();
        if (next.contains("where")) {
          hasWhere = true;
          ctxt.getCounter("words", "where").increment(1);
        } else {
          notWhere = next;
        }
      }
      try {
        ctxt.write(new Text(notWhere), hasWhere ? new LongWritable(1L) : new LongWritable(0L));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void testMapper() throws Exception {
    Pipeline p = new MRPipeline(MapreduceIT.class, tempDir.getDefaultConfiguration());
    Path shakesPath = tempDir.copyResourcePath("shakes.txt");
    PCollection<String> in = p.read(From.textFile(shakesPath));
    PTable<IntWritable, Text> two = in.parallelDo(new MapFn<String, Pair<IntWritable, Text>>() {
      @Override
      public Pair<IntWritable, Text> map(String input) {
        return Pair.of(new IntWritable(input.length()), new Text(input));
      }
    }, Writables.tableOf(Writables.writables(IntWritable.class), Writables.writables(Text.class)));
    
    PTable<IntWritable, Text> out = Mapreduce.map(two, TestMapper.class, IntWritable.class, Text.class);
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
    
    PTable<Text, LongWritable> out = Mapreduce.reduce(two.groupByKey(), TestReducer.class, Text.class, LongWritable.class);
    out.write(To.sequenceFile(tempDir.getPath("temp")));
    PipelineResult res = p.done();
    assertEquals(1, res.getStageResults().size());
    StageResult sr = res.getStageResults().get(0);
    assertEquals(19, sr.getCounters().findCounter("words", "where").getValue());
  }
}
