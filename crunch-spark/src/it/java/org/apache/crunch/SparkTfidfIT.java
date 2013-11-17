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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;

import org.apache.crunch.impl.spark.SparkPipeline;
import org.apache.crunch.io.seq.SeqFileSourceTarget;
import org.apache.crunch.lib.Aggregate;
import org.apache.crunch.lib.Join;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.io.Files;

@SuppressWarnings("serial")
public class SparkTfidfIT implements Serializable {
  @Rule
  public transient TemporaryPath tmpDir = new TemporaryPath();

  // total number of documents, should calculate
  protected static final double N = 2;

  private transient Pipeline pipeline;

  @Before
  public void setUp() throws Exception {
    pipeline = new SparkPipeline("local", "tfidf");

  }
  @Test
  public void testWritablesSingleRun() throws IOException {
    run(pipeline, WritableTypeFamily.getInstance(), true);
  }

  @Test
  public void testWritablesMultiRun() throws IOException {
    run(pipeline, WritableTypeFamily.getInstance(), false);
  }

  /**
   * This method should generate a TF-IDF score for the input.
   */
  public PTable<String, Collection<Pair<String, Double>>> generateTFIDF(PCollection<String> docs, Path termFreqPath,
                                                                        PTypeFamily ptf) throws IOException {

    /*
     * Input: String Input title text
     *
     * Output: PTable<Pair<String, String>, Long> Pair<Pair<word, title>, count
     * in title>
     */
    PTable<Pair<String, String>, Long> tf = Aggregate.count(docs.parallelDo("term document frequency",
        new DoFn<String, Pair<String, String>>() {
          @Override
          public void process(String doc, Emitter<Pair<String, String>> emitter) {
            String[] kv = doc.split("\t");
            String title = kv[0];
            String text = kv[1];
            for (String word : text.split("\\W+")) {
              if (word.length() > 0) {
                Pair<String, String> pair = Pair.of(word.toLowerCase(), title);
                emitter.emit(pair);
              }
            }
          }
        }, ptf.pairs(ptf.strings(), ptf.strings())));

    tf.write(new SeqFileSourceTarget<Pair<Pair<String, String>, Long>>(termFreqPath, tf.getPType()));

    /*
     * Input: Pair<Pair<String, String>, Long> Pair<Pair<word, title>, count in
     * title>
     *
     * Output: PTable<String, Long> PTable<word, # of docs containing word>
     */
    PTable<String, Long> n = Aggregate.count(tf.parallelDo("little n (# of docs contain word)",
        new DoFn<Pair<Pair<String, String>, Long>, String>() {
          @Override
          public void process(Pair<Pair<String, String>, Long> input, Emitter<String> emitter) {
            emitter.emit(input.first().first());
          }
        }, ptf.strings()));

    /*
     * Input: Pair<Pair<String, String>, Long> Pair<Pair<word, title>, count in
     * title>
     *
     * Output: PTable<String, Pair<String, Long>> PTable<word, Pair<title, count
     * in title>>
     */
    PTable<String, Collection<Pair<String, Long>>> wordDocumentCountPair = tf.parallelDo(
        "transform wordDocumentPairCount",
        new DoFn<Pair<Pair<String, String>, Long>, Pair<String, Collection<Pair<String, Long>>>>() {
          Collection<Pair<String, Long>> buffer;
          String key;

          @Override
          public void process(Pair<Pair<String, String>, Long> input,
                              Emitter<Pair<String, Collection<Pair<String, Long>>>> emitter) {
            Pair<String, String> wordDocumentPair = input.first();
            if (!wordDocumentPair.first().equals(key)) {
              flush(emitter);
              key = wordDocumentPair.first();
              buffer = Lists.newArrayList();
            }
            buffer.add(Pair.of(wordDocumentPair.second(), input.second()));
          }

          protected void flush(Emitter<Pair<String, Collection<Pair<String, Long>>>> emitter) {
            if (buffer != null) {
              emitter.emit(Pair.of(key, buffer));
              buffer = null;
            }
          }

          @Override
          public void cleanup(Emitter<Pair<String, Collection<Pair<String, Long>>>> emitter) {
            flush(emitter);
          }
        }, ptf.tableOf(ptf.strings(), ptf.collections(ptf.pairs(ptf.strings(), ptf.longs()))));

    PTable<String, Pair<Long, Collection<Pair<String, Long>>>> joinedResults = Join.join(n, wordDocumentCountPair);

    /*
     * Input: Pair<String, Pair<Long, Collection<Pair<String, Long>>> Pair<word,
     * Pair<# of docs containing word, Collection<Pair<title, term frequency>>>
     *
     * Output: Pair<String, Collection<Pair<String, Double>>> Pair<word,
     * Collection<Pair<title, tfidf>>>
     */
    return joinedResults
        .mapValues(
            new MapFn<Pair<Long, Collection<Pair<String, Long>>>, Collection<Pair<String, Double>>>() {
              @Override
              public Collection<Pair<String, Double>> map(
                  Pair<Long, Collection<Pair<String, Long>>> input) {
                Collection<Pair<String, Double>> tfidfs = Lists.newArrayList();
                double n = input.first();
                double idf = Math.log(N / n);
                for (Pair<String, Long> tf : input.second()) {
                  double tfidf = tf.second() * idf;
                  tfidfs.add(Pair.of(tf.first(), tfidf));
                }
                return tfidfs;
              }

            }, ptf.collections(ptf.pairs(ptf.strings(), ptf.doubles())));
  }

  public void run(Pipeline pipeline, PTypeFamily typeFamily, boolean singleRun) throws IOException {
    String inputFile = tmpDir.copyResourceFileName("docs.txt");
    String outputPath1 = tmpDir.getFileName("output1");
    String outputPath2 = tmpDir.getFileName("output2");

    Path tfPath = tmpDir.getPath("termfreq");

    PCollection<String> docs = pipeline.readTextFile(inputFile);

    PTable<String, Collection<Pair<String, Double>>> results = generateTFIDF(docs, tfPath, typeFamily);
    pipeline.writeTextFile(results, outputPath1);
    if (!singleRun) {
      pipeline.run();
    }

    PTable<String, Collection<Pair<String, Double>>> uppercased = results.mapKeys(
        new MapFn<String, String>() {
          @Override
          public String map(String k1) {
            return k1.toUpperCase();
          }
        }, results.getKeyType());
    pipeline.writeTextFile(uppercased, outputPath2);
    pipeline.done();

    // Check the lowercase version...
    File outputFile = new File(outputPath1, "part-r-00000");
    List<String> lines = Files.readLines(outputFile, Charset.defaultCharset());
    boolean passed = false;
    for (String line : lines) {
      if (line.startsWith("[the") && line.contains("B,0.6931471805599453")) {
        passed = true;
        break;
      }
    }
    assertTrue(passed);

    // ...and the uppercase version
    outputFile = new File(outputPath2, "part-r-00000");
    lines = Files.readLines(outputFile, Charset.defaultCharset());
    passed = false;
    for (String line : lines) {
      if (line.startsWith("[THE") && line.contains("B,0.6931471805599453")) {
        passed = true;
        break;
      }
    }
    assertTrue(passed);
  }
}
