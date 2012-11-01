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
package org.apache.crunch.examples;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.crunch.Aggregator;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@SuppressWarnings("serial")
public class TotalBytesByIP extends Configured implements Tool, Serializable {
  static enum COUNTERS {
    NO_MATCH,
    CORRUPT_SIZE
  }

  static final String logRegex = "^([\\w.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";

  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println();
      System.err.println("Two and only two arguments are accepted.");
      System.err.println("Usage: " + this.getClass().getName() + " [generic options] input output");
      System.err.println();
      GenericOptionsParser.printGenericCommandUsage(System.err);
      return 1;
    }
    // Create an object to coordinate pipeline creation and execution.
    Pipeline pipeline = new MRPipeline(TotalBytesByIP.class, getConf());
    // Reference a given text file as a collection of Strings.
    PCollection<String> lines = pipeline.readTextFile(args[0]);

    // Aggregator used for summing up response size
    Aggregator<Long> agg = Aggregators.SUM_LONGS();

    // Table of (ip, sum(response size))
    PTable<String, Long> ipAddrResponseSize = lines
        .parallelDo(extractIPResponseSize, Writables.tableOf(Writables.strings(), Writables.longs())).groupByKey()
        .combineValues(agg);

    pipeline.writeTextFile(ipAddrResponseSize, args[1]);
    // Execute the pipeline as a MapReduce.
    PipelineResult result = pipeline.done();

    return result.succeeded() ? 0 : 1;
  }

  // Function to parse apache log records
  // Given a standard apache log line, extract the ip address and
  // request size. Outputs the ip and response size.
  //
  // Input: 55.1.3.2 ...... 200 512 ....
  // Output: (55.1.3.2, 512)
  DoFn<String, Pair<String, Long>> extractIPResponseSize = new DoFn<String, Pair<String, Long>>() {
    transient Pattern pattern;

    public void initialize() {
      pattern = Pattern.compile(logRegex);
    }

    public void process(String line, Emitter<Pair<String, Long>> emitter) {
      Matcher matcher = pattern.matcher(line);
      if (matcher.matches()) {
        try {
          Long requestSize = Long.parseLong(matcher.group(7));
          String remoteAddr = matcher.group(1);
          emitter.emit(Pair.of(remoteAddr, requestSize));
        } catch (NumberFormatException e) {
          // corrupt line, we should increment counter
        }
      }
    }
  };

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new Configuration(), new TotalBytesByIP(), args);
    System.exit(result);
  }
}
