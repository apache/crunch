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

import static org.apache.crunch.fn.Aggregators.SUM_LONGS;
import static org.apache.crunch.fn.Aggregators.pairAggregator;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.crunch.Aggregator;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@SuppressWarnings("serial")
public class AverageBytesByIP extends Configured implements Tool, Serializable {
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
    Pipeline pipeline = new MRPipeline(AverageBytesByIP.class, getConf());
    // Reference a given text file as a collection of Strings.
    PCollection<String> lines = pipeline.readTextFile(args[0]);

    // Aggregator used for summing up response size and count
    Aggregator<Pair<Long, Long>> agg = pairAggregator(SUM_LONGS(), SUM_LONGS());

    // Table of (ip, sum(response size), count)
    PTable<String, Pair<Long, Long>> remoteAddrResponseSize = lines
        .parallelDo(extractResponseSize,
            Writables.tableOf(Writables.strings(), Writables.pairs(Writables.longs(), Writables.longs()))).groupByKey()
        .combineValues(agg);

    // Calculate average response size by ip address
    PTable<String, Double> avgs = remoteAddrResponseSize.parallelDo(calulateAverage,
        Writables.tableOf(Writables.strings(), Writables.doubles()));

    // write the result to a text file
    pipeline.writeTextFile(avgs, args[1]);
    // Execute the pipeline as a MapReduce.
    PipelineResult result = pipeline.done();

    return result.succeeded() ? 0 : 1;
  }

  // Function to calculate the average response size for a given ip address
  //
  // Input: (ip, sum(response size), count)
  // Output: (ip, average response size)
  MapFn<Pair<String, Pair<Long, Long>>, Pair<String, Double>> calulateAverage = new MapFn<Pair<String, Pair<Long, Long>>, Pair<String, Double>>() {
    @Override
    public Pair<String, Double> map(Pair<String, Pair<Long, Long>> arg) {
      Pair<Long, Long> sumCount = arg.second();
      double avg = 0;
      if (sumCount.second() > 0) {
        avg = (double) sumCount.first() / (double) sumCount.second();
      }
      return Pair.of(arg.first(), avg);
    }
  };

  // Function to parse apache log records
  // Given a standard apache log line, extract the ip address and
  // response size. Outputs ip and the response size and a count (1) so that
  // a combiner can be used.
  //
  // Input: 55.1.3.2 ...... 200 512 ....
  // Output: (55.1.3.2, (512, 1))
  DoFn<String, Pair<String, Pair<Long, Long>>> extractResponseSize = new DoFn<String, Pair<String, Pair<Long, Long>>>() {
    transient Pattern pattern;

    public void initialize() {
      pattern = Pattern.compile(logRegex);
    }

    public void process(String line, Emitter<Pair<String, Pair<Long, Long>>> emitter) {
      Matcher matcher = pattern.matcher(line);
      if (matcher.matches()) {
        try {
          Long responseSize = Long.parseLong(matcher.group(7));
          Pair<Long, Long> sumCount = Pair.of(responseSize, 1L);
          String remoteAddr = matcher.group(1);
          emitter.emit(Pair.of(remoteAddr, sumCount));
        } catch (NumberFormatException e) {
          this.increment(COUNTERS.CORRUPT_SIZE);
        }
      } else {
        this.increment(COUNTERS.NO_MATCH);
      }
    }
  };

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new Configuration(), new AverageBytesByIP(), args);
    System.exit(result);
  }
}
