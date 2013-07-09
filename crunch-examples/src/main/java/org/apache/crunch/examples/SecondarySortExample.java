/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.crunch.examples;

import java.io.Serializable;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.SecondarySort;
import org.apache.crunch.io.To;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Splitter;

@SuppressWarnings("serial")
public class SecondarySortExample extends Configured implements Tool, Serializable {

  static enum COUNTERS {
    CORRUPT_TIMESTAMP,
    CORRUPT_LINE
  }

  // Input records are comma separated. The first field is grouping record. The
  // second is the one to sort on (a long in this implementation). The rest is
  // payload to be sorted.

  // For example:

  // one,1,1
  // one,2,-3
  // two,4,5
  // two,2,6
  // two,1,7,9
  // three,0,-1
  // one,-5,10
  // one,-10,garbage

  private static final char SPLIT_ON = ',';
  private static final Splitter INPUT_SPLITTER = Splitter.on(SPLIT_ON).trimResults().omitEmptyStrings().limit(3);

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println();
      System.err.println("Usage: " + this.getClass().getName()
          + " [generic options] input output");
      System.err.println();
      GenericOptionsParser.printGenericCommandUsage(System.err);
      return 1;
    }
    // Create an object to coordinate pipeline creation and execution.
    Pipeline pipeline = new MRPipeline(SecondarySortExample.class, getConf());
    // Reference a given text file as a collection of Strings.
    PCollection<String> lines = pipeline.readTextFile(args[0]);

    // Define a function that parses each line in a PCollection of Strings into
    // a pair of pairs, the first of which will be grouped by (first member) and
    // the sorted by (second memeber). The second pair is payload which can be
    // passed in an Iterable object.
    PTable<String, Pair<Long, String>> pairs = lines.parallelDo("extract_records",
        new DoFn<String, Pair<String, Pair<Long, String>>>() {
          @Override
          public void process(String line, Emitter<Pair<String, Pair<Long, String>>> emitter) {
            int i = 0;
            String key = "";
            long timestamp = 0;
            String value = "";
            for (String element : INPUT_SPLITTER.split(line)) {
              switch (++i) {
              case 1:
                key = element;
                break;
              case 2:
                try {
                  timestamp = Long.parseLong(element);
                } catch (NumberFormatException e) {
                  System.out.println("Timestamp not in long format '" + line + "'");
                  this.increment(COUNTERS.CORRUPT_TIMESTAMP);
                }
                break;
              case 3:
                value = element;
                break;
              default:
                System.err.println("i = " + i + " should never happen!");
                break;
              }
            }
            if (i == 3) {
              Long sortby = new Long(timestamp);
              emitter.emit(Pair.of(key, Pair.of(sortby, value)));
            } else {
              this.increment(COUNTERS.CORRUPT_LINE);
            }
          }}, Avros.tableOf(Avros.strings(), Avros.pairs(Avros.longs(), Avros.strings())));

    // The output of the above input will be (with one reducer):

    // one : [[-10,garbage],[-5,10],[1,1],[2,-3]]
    // three : [[0,-1]]
    // two : [[1,7,9],[2,6],[4,5]]

    SecondarySort.sortAndApply(pairs,
        new DoFn<Pair<String, Iterable<Pair<Long, String>>>, String>() {
          final StringBuilder sb = new StringBuilder();
          @Override
          public void process(Pair<String, Iterable<Pair<Long, String>>> input, Emitter<String> emitter) {
            sb.setLength(0);
            sb.append(input.first());
            sb.append(" : [");
            boolean first = true;
            for(Pair<Long, String> pair : input.second()) {
              if (first) {
                first = false;
              } else {
                sb.append(',');
              }
              sb.append(pair);
            }
            sb.append("]");
            emitter.emit(sb.toString());
          }
        }, Writables.strings()).write(To.textFile(args[1]));

    // Execute the pipeline as a MapReduce.
    return pipeline.done().succeeded() ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = -1;
    try {
      exitCode = ToolRunner.run(new Configuration(), new SecondarySortExample(), args);
    } catch (Throwable e) {
      e.printStackTrace();
    }
    System.exit(exitCode);
  }
}
