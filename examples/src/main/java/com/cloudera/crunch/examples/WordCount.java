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
package com.cloudera.crunch.examples;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.lib.Aggregate;
import com.cloudera.crunch.type.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.Serializable;

public class WordCount extends Configured implements Tool, Serializable {
  public int run(String[] args) throws Exception {
    Pipeline pipeline = new MRPipeline(WordCount.class);    
    PCollection<String> words = pipeline.readTextFile(args[0]);

    PTable<String, Long> counts = Aggregate.count(words.parallelDo("split",
        new DoFn<String, String>() {
          @Override
          public void process(String line, Emitter<String> emitter) {
            for (String word : line.split("\\s+")) {
              emitter.emit(word);
            }
          }
        }, Writables.strings()));

    pipeline.writeTextFile(counts, args[1]);
    pipeline.run();
    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new WordCount(), args);
  }
}
