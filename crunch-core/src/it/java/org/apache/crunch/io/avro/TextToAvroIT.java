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
package org.apache.crunch.io.avro;

import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.junit.Rule;
import org.junit.Test;

public class TextToAvroIT {
  @Rule
  public transient TemporaryPath tmpDir = TemporaryPaths.create();
  
  @Test(expected=CrunchRuntimeException.class)
  public void testTextToAvro() throws Exception {
    String shakes = tmpDir.copyResourceFileName("shakes.txt");
    Pipeline pipeline = new MRPipeline(TextToAvroIT.class, tmpDir.getDefaultConfiguration());
    pipeline.read(From.textFile(shakes)).write(To.avroFile("output"));
    pipeline.run();
  }
}
