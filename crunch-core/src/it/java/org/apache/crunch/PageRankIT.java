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

import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.Aggregate;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.PTypes;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class PageRankIT {

  private static List<String> URLS = ImmutableList.of(
          "www.A.com       www.B.com",
          "www.A.com       www.C.com",
          "www.A.com       www.D.com",
          "www.A.com       www.E.com",
          "www.B.com       www.D.com",
          "www.B.com       www.E.com",
          "www.C.com       www.D.com",
          "www.D.com       www.B.com",
          "www.E.com       www.A.com",
          "www.F.com       www.B.com",
          "www.F.com       www.C.com");

  public static class PageRankData {
    public float score;
    public float lastScore;
    public List<String> urls;

    public PageRankData() {
    }

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

  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testAvroReflect() throws Exception {
    PTypeFamily tf = AvroTypeFamily.getInstance();
    PType<PageRankData> prType = Avros.reflects(PageRankData.class);
    run(new MRPipeline(PageRankIT.class, tmpDir.getDefaultConfiguration()),
        prType, tf);
  }

  @Test
  public void testAvroMReflectInMemory() throws Exception {
    PTypeFamily tf = AvroTypeFamily.getInstance();
    PType<PageRankData> prType = Avros.reflects(PageRankData.class);
    run(MemPipeline.getInstance(), prType, tf);
  }

  @Test
  public void testAvroJSON() throws Exception {
    PTypeFamily tf = AvroTypeFamily.getInstance();
    PType<PageRankData> prType = PTypes.jsonString(PageRankData.class, tf);
    run(new MRPipeline(PageRankIT.class, tmpDir.getDefaultConfiguration()),
        prType, tf);
  }

  @Test
  public void testWritablesJSON() throws Exception {
    PTypeFamily tf = WritableTypeFamily.getInstance();
    PType<PageRankData> prType = PTypes.jsonString(PageRankData.class, tf);
    run(new MRPipeline(PageRankIT.class, tmpDir.getDefaultConfiguration()),
        prType, tf);
  }

  public static PTable<String, PageRankData> pageRank(PTable<String, PageRankData> input, final float d) {
    PTypeFamily ptf = input.getTypeFamily();
    PTable<String, Float> outbound = input.parallelDo(new DoFn<Pair<String, PageRankData>, Pair<String, Float>>() {
      @Override
      public void process(Pair<String, PageRankData> input, Emitter<Pair<String, Float>> emitter) {
        PageRankData prd = input.second();
        for (String link : prd.urls) {
          emitter.emit(Pair.of(link, prd.propagatedScore()));
        }
      }
    }, ptf.tableOf(ptf.strings(), ptf.floats()));

    return input.cogroup(outbound).mapValues(
        new MapFn<Pair<Collection<PageRankData>, Collection<Float>>, PageRankData>() {
          @Override
          public PageRankData map(Pair<Collection<PageRankData>, Collection<Float>> input) {
            PageRankData prd = Iterables.getOnlyElement(input.first());
            Collection<Float> propagatedScores = input.second();
            float sum = 0.0f;
            for (Float s : propagatedScores) {
              sum += s;
            }
            return prd.next(d + (1.0f - d) * sum);
          }
        }, input.getValueType());
  }

  public static void run(Pipeline pipeline,
      PType<PageRankData> prType, PTypeFamily ptf) throws Exception {
    PTable<String, PageRankData> scores = pipeline.create(URLS, ptf.strings())
        .parallelDo(new MapFn<String, Pair<String, String>>() {
          @Override
          public Pair<String, String> map(String input) {
            String[] urls = input.split("\\s+");
            return Pair.of(urls[0], urls[1]);
          }
        }, ptf.tableOf(ptf.strings(), ptf.strings())).groupByKey()
        .mapValues(new MapFn<Iterable<String>, PageRankData>() {
          @Override
          public PageRankData map(Iterable<String> input) {
            return new PageRankData(1.0f, 0.0f, input);
          }
        }, prType);

    Float delta = 1.0f;
    while (delta > 0.01) {
      scores = pageRank(scores, 0.5f);
      scores.materialize().iterator(); // force the write
      delta = Aggregate.max(scores.parallelDo(new MapFn<Pair<String, PageRankData>, Float>() {
        @Override
        public Float map(Pair<String, PageRankData> input) {
          PageRankData prd = input.second();
          return Math.abs(prd.score - prd.lastScore);
        }
      }, ptf.floats())).getValue();
    }
    assertEquals(0.0048, delta, 0.001);
  }
}
