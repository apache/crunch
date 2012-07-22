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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.io.Files;


public class TupleNClassCastBugIT {
  @Rule
  public TemporaryPath tmpDir = new TemporaryPath();

  public static PCollection<TupleN> mapGroupDo(PCollection<String> lines, PTypeFamily ptf) {
    PTable<String, TupleN> mapped = lines.parallelDo(new MapFn<String, Pair<String, TupleN>>() {

      @Override
      public Pair<String, TupleN> map(String line) {
        String[] columns = line.split("\\t");
        String docId = columns[0];
        String docLine = columns[1];
        return Pair.of(docId, new TupleN(docId, docLine));
      }
    }, ptf.tableOf(ptf.strings(), ptf.tuples(ptf.strings(), ptf.strings())));
    return mapped.groupByKey().parallelDo(new DoFn<Pair<String, Iterable<TupleN>>, TupleN>() {
      @Override
      public void process(Pair<String, Iterable<TupleN>> input, Emitter<TupleN> tupleNEmitter) {
        for (TupleN tuple : input.second()) {
          tupleNEmitter.emit(tuple);
        }
      }
    }, ptf.tuples(ptf.strings(), ptf.strings()));
  }

  @Test
  public void testWritables() throws IOException {
    run(new MRPipeline(TupleNClassCastBugIT.class, tmpDir.setTempLoc(new Configuration())), WritableTypeFamily.getInstance());
  }

  @Test
  public void testAvro() throws IOException {
    run(new MRPipeline(TupleNClassCastBugIT.class, tmpDir.setTempLoc(new Configuration())), AvroTypeFamily.getInstance());
  }

  public void run(Pipeline pipeline, PTypeFamily typeFamily) throws IOException {
    String inputPath = tmpDir.copyResourceFileName("docs.txt");
    String outputPath = tmpDir.getFileName("output");

    PCollection<String> docLines = pipeline.readTextFile(inputPath);
    pipeline.writeTextFile(mapGroupDo(docLines, typeFamily), outputPath);
    pipeline.done();

    // *** We are not directly testing the output, we are looking for a
    // ClassCastException
    // *** which is thrown in a different thread during the reduce phase. If all
    // is well
    // *** the file will exist and have six lines. Otherwise the bug is present.
    File outputFile = new File(outputPath, "part-r-00000");
    List<String> lines = Files.readLines(outputFile, Charset.defaultCharset());
    int lineCount = 0;
    for (String line : lines) {
      lineCount++;
    }
    assertEquals(6, lineCount);
  }
}
