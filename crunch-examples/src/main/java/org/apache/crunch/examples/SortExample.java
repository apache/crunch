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

import org.apache.crunch.PCollection;
import org.apache.crunch.lib.Sort;
import org.apache.crunch.lib.Sort.Order;
import org.apache.crunch.util.CrunchTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**
 * Simple Crunch tool for running sorting examples from the command line.
 */
public class SortExample extends CrunchTool {

  public SortExample() {
    super();
  }
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: <input-path> <output-path> <num-reducers>");
      return 1;
    }
    
    PCollection<String> in = readTextFile(args[0]);
    writeTextFile(Sort.sort(in, Integer.valueOf(args[2]), Order.ASCENDING), args[1]);
    done();

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new SortExample(), args);
  }
}
