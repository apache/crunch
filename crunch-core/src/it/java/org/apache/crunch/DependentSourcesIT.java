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

import static org.apache.crunch.types.avro.Avros.strings;
import static org.apache.crunch.types.avro.Avros.tableOf;

import java.util.List;

import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRJob;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.impl.mr.MRPipelineExecution;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.materialize.MaterializableIterable;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class DependentSourcesIT {

  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testRun() throws Exception {
    run(new MRPipeline(DependentSourcesIT.class, tmpDir.getDefaultConfiguration()),
        tmpDir.copyResourcePath("shakes.txt"),
        tmpDir.getFileName("out"));
  }

  public static void run(MRPipeline p, Path inputPath, String out) throws Exception {
     PCollection<String> in = p.read(From.textFile(inputPath));
     PTable<String, String> op = in.parallelDo("op1", new DoFn<String, Pair<String, String>>() {
      @Override
      public void process(String input, Emitter<Pair<String, String>> emitter) {
        if (input.length() > 5) {
          emitter.emit(Pair.of(input.substring(0, 3), input));
        }
      } 
     }, tableOf(strings(), strings()));
     
     ReadableData<Pair<String, String>> rd = op.asReadable(true);
     
     op = op.parallelDo("op2", IdentityFn.<Pair<String,String>>getInstance(), tableOf(strings(), strings()),
         ParallelDoOptions.builder().sourceTargets(rd.getSourceTargets()).build());
     
     PCollection<String> output = op.values();
     output.write(To.textFile(out));
     MRPipelineExecution exec = p.runAsync();
     exec.waitUntilDone();
     List<MRJob> jobs = exec.getJobs();
     Assert.assertEquals(2, jobs.size());
     Assert.assertEquals(0, jobs.get(0).getJob().getNumReduceTasks());
     Assert.assertEquals(0, jobs.get(1).getJob().getNumReduceTasks());     
  }
}
