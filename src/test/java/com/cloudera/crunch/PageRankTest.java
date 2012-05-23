/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.crunch;

import static org.junit.Assert.assertEquals;

import com.cloudera.crunch.impl.mem.MemPipeline;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.lib.Aggregate;
import com.cloudera.crunch.test.FileHelper;
import com.cloudera.crunch.types.PType;
import com.cloudera.crunch.types.PTypeFamily;
import com.cloudera.crunch.types.avro.AvroTypeFamily;
import com.cloudera.crunch.types.avro.Avros;
import com.cloudera.crunch.types.writable.WritableTypeFamily;
import com.cloudera.crunch.util.PTypes;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

import org.junit.Test;

public class PageRankTest {

  public static class PageRankData {
	public float score;
	public float lastScore;
	public List<String> urls;
	
	public PageRankData() { }
	
	public PageRankData(float score, float lastScore, Iterable<String> urls) {
	  this.score = score;
	  this.lastScore = lastScore;
	  this.urls = Lists.newArrayList(urls);
	}
	
	public PageRankData next(float newScore) {
	  return new PageRankData(newScore, score, urls);
	}
	
	public float propagatedScore() {
	  return score / urls.size();
	}
	
	@Override
	public String toString() {
	  return score + " " + lastScore + " " + urls;
	}
  }
  
  @Test public void testAvroReflect() throws Exception {
	PTypeFamily tf = AvroTypeFamily.getInstance();
	PType<PageRankData> prType = Avros.reflects(PageRankData.class);
    run(new MRPipeline(PageRankTest.class), prType, tf);	
  }
  
  @Test public void testAvroMReflectInMemory() throws Exception {
    PTypeFamily tf = AvroTypeFamily.getInstance();
    PType<PageRankData> prType = Avros.reflects(PageRankData.class);
    run(MemPipeline.getInstance(), prType, tf);        
  }
  
  @Test public void testAvroJSON() throws Exception {
	PTypeFamily tf = AvroTypeFamily.getInstance();
	PType<PageRankData> prType = PTypes.jsonString(PageRankData.class, tf);
    run(new MRPipeline(PageRankTest.class), prType, tf);
  }

  @Test public void testAvroBSON() throws Exception {
	PTypeFamily tf = AvroTypeFamily.getInstance();
	PType<PageRankData> prType = PTypes.smile(PageRankData.class, tf);
    run(new MRPipeline(PageRankTest.class), prType, tf);
  }
  
  @Test public void testWritablesJSON() throws Exception {
	PTypeFamily tf = WritableTypeFamily.getInstance();
	PType<PageRankData> prType = PTypes.jsonString(PageRankData.class, tf);
    run(new MRPipeline(PageRankTest.class), prType, tf);
  }

  @Test public void testWritablesBSON() throws Exception {
	PTypeFamily tf = WritableTypeFamily.getInstance();
	PType<PageRankData> prType = PTypes.smile(PageRankData.class, tf);
    run(new MRPipeline(PageRankTest.class), prType, tf);
  }
  
  public static PTable<String, PageRankData> pageRank(PTable<String, PageRankData> input, final float d) {
    PTypeFamily ptf = input.getTypeFamily();
    PTable<String, Float> outbound = input.parallelDo(
        new DoFn<Pair<String, PageRankData>, Pair<String, Float>>() {
          @Override
          public void process(Pair<String, PageRankData> input, Emitter<Pair<String, Float>> emitter) {
            PageRankData prd = input.second();
            for (String link : prd.urls) {
              emitter.emit(Pair.of(link, prd.propagatedScore()));
            }
          }
        }, ptf.tableOf(ptf.strings(), ptf.floats()));
    
    return input.cogroup(outbound).parallelDo(
        new MapFn<Pair<String, Pair<Collection<PageRankData>, Collection<Float>>>, Pair<String, PageRankData>>() {
              @Override
              public Pair<String, PageRankData> map(Pair<String, Pair<Collection<PageRankData>, Collection<Float>>> input) {
                PageRankData prd = Iterables.getOnlyElement(input.second().first());
                Collection<Float> propagatedScores = input.second().second();
                float sum = 0.0f;
                for (Float s : propagatedScores) {
                  sum += s;
                }
                return Pair.of(input.first(), prd.next(d + (1.0f - d)*sum));
              }
            }, input.getPTableType());
  }
  
  public static void run(Pipeline pipeline, PType<PageRankData> prType, PTypeFamily ptf) throws Exception {
    String urlInput = FileHelper.createTempCopyOf("urls.txt");
    PTable<String, PageRankData> scores = pipeline.readTextFile(urlInput)
        .parallelDo(new MapFn<String, Pair<String, String>>() {
          @Override
          public Pair<String, String> map(String input) {
            String[] urls = input.split("\\t");
            return Pair.of(urls[0], urls[1]);
          }
        }, ptf.tableOf(ptf.strings(), ptf.strings()))
        .groupByKey()
        .parallelDo(new MapFn<Pair<String, Iterable<String>>, Pair<String, PageRankData>>() {
              @Override
              public Pair<String, PageRankData> map(Pair<String, Iterable<String>> input) {
                return Pair.of(input.first(), new PageRankData(1.0f, 0.0f, input.second()));
              }
            }, ptf.tableOf(ptf.strings(), prType));
    
    Float delta = 1.0f;
    while (delta > 0.01) {
      scores = pageRank(scores, 0.5f);
      scores.materialize().iterator(); // force the write
      delta = Iterables.getFirst(Aggregate.max(
          scores.parallelDo(new MapFn<Pair<String, PageRankData>, Float>() {
            @Override
            public Float map(Pair<String, PageRankData> input) {
              PageRankData prd = input.second();
              return Math.abs(prd.score - prd.lastScore);
            }
          }, ptf.floats())).materialize(), null);
    }
    assertEquals(0.0048, delta, 0.001);
  }
}
