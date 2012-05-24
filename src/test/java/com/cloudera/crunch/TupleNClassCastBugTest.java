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

import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.lib.Aggregate;
import com.cloudera.crunch.types.PTypeFamily;
import com.cloudera.crunch.types.avro.AvroTypeFamily;
import com.cloudera.crunch.types.writable.WritableTypeFamily;
import com.google.common.io.Files;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.newInputStreamSupplier;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class TupleNClassCastBugTest {

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
    run(new MRPipeline(TupleNClassCastBugTest.class), WritableTypeFamily.getInstance());
  }

  @Test
  public void testAvro() throws IOException {
    run(new MRPipeline(TupleNClassCastBugTest.class), AvroTypeFamily.getInstance());
  }

  public void run(Pipeline pipeline, PTypeFamily typeFamily) throws IOException {
    File input = File.createTempFile("docs", "txt");
    input.deleteOnExit();
    Files.copy(newInputStreamSupplier(getResource("docs.txt")), input);

    File output = File.createTempFile("output", "");
    String outputPath = output.getAbsolutePath();
    output.delete();

    PCollection<String> docLines = pipeline.readTextFile(input.getAbsolutePath());
    pipeline.writeTextFile(mapGroupDo(docLines, typeFamily), outputPath);
    pipeline.done();

    // *** We are not directly testing the output, we are looking for a ClassCastException
    // *** which is thrown in a different thread during the reduce phase. If all is well
    // *** the file will exist and have six lines. Otherwise the bug is present.
    File outputFile = new File(output, "part-r-00000");
    List<String> lines = Files.readLines(outputFile, Charset.defaultCharset());
    int lineCount = 0;
    for (String line : lines) {
      lineCount++;
    }
    assertEquals(6, lineCount);
    output.deleteOnExit();
  }
}
