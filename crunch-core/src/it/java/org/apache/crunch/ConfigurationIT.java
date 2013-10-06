/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.apache.crunch;

import junit.framework.Assert;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;

public class ConfigurationIT {

  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  private static final String KEY = "key";

  private static DoFn<String, String> CONFIG_FN = new DoFn<String, String>() {
    private String value;

    @Override
    public void configure(Configuration conf) {
      this.value = conf.get(KEY, "none");
    }

    @Override
    public void process(String input, Emitter<String> emitter) {
      emitter.emit(value);
    }
  };

  @Test
  public void testRun() throws Exception {
    run(new MRPipeline(ConfigurationIT.class, tmpDir.getDefaultConfiguration()),
        tmpDir.copyResourceFileName("set1.txt"), "testapalooza");
  }

  private static void run(Pipeline p, String input, String expected) throws Exception {
    Iterable<String> mat = p.read(From.textFile(input))
        .parallelDo("conf", CONFIG_FN, Writables.strings(), ParallelDoOptions.builder().conf(KEY, expected).build())
        .materialize();
    for (String v : mat) {
      if (!expected.equals(v)) {
        Assert.fail("Unexpected value: " + v);
      }
    }
    p.done();
  }
}