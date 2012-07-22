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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.At;
import org.apache.crunch.io.To;
import org.apache.crunch.lib.Aggregate;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class WordCountIT {
  @Rule
  public TemporaryPath tmpDir = new TemporaryPath();

  enum WordCountStats {
    ANDS
  };

  public static PTable<String, Long> wordCount(PCollection<String> words, PTypeFamily typeFamily) {
    return Aggregate.count(words.parallelDo(new DoFn<String, String>() {

      @Override
      public void process(String line, Emitter<String> emitter) {
        for (String word : line.split("\\s+")) {
          emitter.emit(word);
          if ("and".equals(word)) {
            increment(WordCountStats.ANDS);
          }
        }
      }
    }, typeFamily.strings()));
  }

  public static PTable<String, Long> substr(PTable<String, Long> ptable) {
    return ptable.parallelDo(new DoFn<Pair<String, Long>, Pair<String, Long>>() {

      public void process(Pair<String, Long> input, Emitter<Pair<String, Long>> emitter) {
        if (input.first().length() > 0) {
          emitter.emit(Pair.of(input.first().substring(0, 1), input.second()));
        }
      }
    }, ptable.getPTableType());
  }

  private boolean runSecond = false;
  private boolean useToOutput = false;

  @Test
  public void testWritables() throws IOException {
    run(new MRPipeline(WordCountIT.class, tmpDir.setTempLoc(new Configuration())), WritableTypeFamily.getInstance());
  }

  @Test
  public void testWritablesWithSecond() throws IOException {
    runSecond = true;
    run(new MRPipeline(WordCountIT.class, tmpDir.setTempLoc(new Configuration())), WritableTypeFamily.getInstance());
  }

  @Test
  public void testWritablesWithSecondUseToOutput() throws IOException {
    runSecond = true;
    useToOutput = true;
    run(new MRPipeline(WordCountIT.class, tmpDir.setTempLoc(new Configuration())), WritableTypeFamily.getInstance());
  }

  @Test
  public void testAvro() throws IOException {
    run(new MRPipeline(WordCountIT.class, tmpDir.setTempLoc(new Configuration())), AvroTypeFamily.getInstance());
  }

  @Test
  public void testAvroWithSecond() throws IOException {
    runSecond = true;
    run(new MRPipeline(WordCountIT.class, tmpDir.setTempLoc(new Configuration())), AvroTypeFamily.getInstance());
  }

  @Test
  public void testWithTopWritable() throws IOException {
    runWithTop(WritableTypeFamily.getInstance());
  }

  @Test
  public void testWithTopAvro() throws IOException {
    runWithTop(AvroTypeFamily.getInstance());
  }

  public void runWithTop(PTypeFamily tf) throws IOException {
    Pipeline pipeline = new MRPipeline(WordCountIT.class, tmpDir.setTempLoc(new Configuration()));
    String inputPath = tmpDir.copyResourceFileName("shakes.txt");

    PCollection<String> shakespeare = pipeline.read(At.textFile(inputPath, tf.strings()));
    PTable<String, Long> wordCount = wordCount(shakespeare, tf);
    List<Pair<String, Long>> top5 = Lists.newArrayList(Aggregate.top(wordCount, 5, true).materialize());
    assertEquals(
        ImmutableList.of(Pair.of("", 1470L), Pair.of("the", 620L), Pair.of("and", 427L), Pair.of("of", 396L),
            Pair.of("to", 367L)), top5);
  }

  public void run(Pipeline pipeline, PTypeFamily typeFamily) throws IOException {
    String inputPath = tmpDir.copyResourceFileName("shakes.txt");
    String outputPath = tmpDir.getFileName("output");

    PCollection<String> shakespeare = pipeline.read(At.textFile(inputPath, typeFamily.strings()));
    PTable<String, Long> wordCount = wordCount(shakespeare, typeFamily);
    if (useToOutput) {
      wordCount.write(To.textFile(outputPath));
    } else {
      pipeline.writeTextFile(wordCount, outputPath);
    }

    if (runSecond) {
      String substrPath = tmpDir.getFileName("substr");
      PTable<String, Long> we = substr(wordCount).groupByKey().combineValues(CombineFn.<String> SUM_LONGS());
      pipeline.writeTextFile(we, substrPath);
    }
    PipelineResult res = pipeline.done();
    assertTrue(res.succeeded());
    List<PipelineResult.StageResult> stageResults = res.getStageResults();
    if (runSecond) {
      assertEquals(2, stageResults.size());
    } else {
      assertEquals(1, stageResults.size());
      assertEquals(427, stageResults.get(0).getCounterValue(WordCountStats.ANDS));
    }

    File outputFile = new File(outputPath, "part-r-00000");
    List<String> lines = Files.readLines(outputFile, Charset.defaultCharset());
    boolean passed = false;
    for (String line : lines) {
      if (line.startsWith("Macbeth\t28")) {
        passed = true;
        break;
      }
    }
    assertTrue(passed);
  }
}
