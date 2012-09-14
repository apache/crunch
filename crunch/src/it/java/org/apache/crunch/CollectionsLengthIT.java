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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.Long;
import java.util.Collection;

import org.apache.crunch.PObject;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

@SuppressWarnings("serial")
public class CollectionsLengthIT {

  public static final Long LINES_IN_SHAKESPEARE = 3667L;

  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testWritables() throws IOException {
    run(new MRPipeline(CollectionsIT.class, tmpDir.getDefaultConfiguration()), WritableTypeFamily.getInstance());
  }

  @Test
  public void testAvro() throws IOException {
    run(new MRPipeline(CollectionsIT.class, tmpDir.getDefaultConfiguration()), AvroTypeFamily.getInstance());
  }

  @Test
  public void testInMemoryWritables() throws IOException {
    run(MemPipeline.getInstance(), WritableTypeFamily.getInstance());
  }

  @Test
  public void testInMemoryAvro() throws IOException {
    run(MemPipeline.getInstance(), AvroTypeFamily.getInstance());
  }

  public void run(Pipeline pipeline, PTypeFamily typeFamily) throws IOException {
    String shakesInputPath = tmpDir.copyResourceFileName("shakes.txt");

    PCollection<String> shakespeare = pipeline.readTextFile(shakesInputPath);
    Long length = shakespeare.length().getValue();
    assertEquals("Incorrect length for shakespear PCollection.", LINES_IN_SHAKESPEARE, length);
  }
}
