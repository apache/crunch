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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.crunch.PipelineResult.StageResult;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class StageResultsCountersIT {

  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  public static HashSet<String> SPECIAL_KEYWORDS = Sets.newHashSet("AND", "OR", "NOT");

  public static String KEYWORDS_COUNTER_GROUP = "KEYWORDS_COUNTER_GROUP";

  @After
  public void after() {
    MemPipeline.clearCounters();
  }
  
  @Test
  public void testStageResultsCountersMRWritables() throws Exception {
    testSpecialKeywordCount(new MRPipeline(StageResultsCountersIT.class, tmpDir.getDefaultConfiguration()),
        WritableTypeFamily.getInstance());
  }

  @Test
  public void testStageResultsCountersMRAvro() throws Exception {
    testSpecialKeywordCount(new MRPipeline(StageResultsCountersIT.class, tmpDir.getDefaultConfiguration()),
        AvroTypeFamily.getInstance());
  }

  @Test
  public void testStageResultsCountersMemWritables() throws Exception {
    testSpecialKeywordCount(MemPipeline.getInstance(), WritableTypeFamily.getInstance());
  }

  @Test
  public void testStageResultsCountersMemAvro() throws Exception {
    testSpecialKeywordCount(MemPipeline.getInstance(), AvroTypeFamily.getInstance());
  }

  public void testSpecialKeywordCount(Pipeline pipeline, PTypeFamily tf) throws Exception {

    String rowsInputPath = tmpDir.copyResourceFileName("shakes.txt");

    PipelineResult result = coutSpecialKeywords(pipeline, rowsInputPath, tf);

    assertTrue(result.succeeded());

    Map<String, Long> keywordsMap = countersToMap(result.getStageResults(), KEYWORDS_COUNTER_GROUP);

    assertThat(keywordsMap, is((Map<String, Long>) ImmutableMap.of("NOT", 157L, "AND", 596L, "OR", 81L)));
  }

  private static PipelineResult coutSpecialKeywords(Pipeline pipeline, String inputFileName, PTypeFamily tf) {

    pipeline.read(From.textFile(inputFileName)).parallelDo(new DoFn<String, Void>() {

      @Override
      public void process(String text, Emitter<Void> emitter) {

        if (!StringUtils.isBlank(text)) {

          String[] tokens = text.toUpperCase().split("\\s");

          for (String token : tokens) {
            if (SPECIAL_KEYWORDS.contains(token)) {
              increment(KEYWORDS_COUNTER_GROUP, token);
            }
          }
        }
      }
    }, tf.nulls()).materialize(); // TODO can we avoid the materialize ?

    return pipeline.done();
  }

  private static Map<String, Long> countersToMap(List<StageResult> stages, String counterGroupName) {

    Map<String, Long> countersMap = Maps.newHashMap();

    for (StageResult sr : stages) {
      for (String counterName : sr.getCounterNames().get(counterGroupName)) {
        countersMap.put(sr.getCounterDisplayName(counterGroupName, counterName),
            sr.getCounterValue(counterGroupName, counterName));
      }
    }

    return countersMap;
  }

}
