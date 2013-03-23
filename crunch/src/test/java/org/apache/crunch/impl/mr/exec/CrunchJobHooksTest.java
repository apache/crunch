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
package org.apache.crunch.impl.mr.exec;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class CrunchJobHooksTest {

  @Test
  public void testExtractPartitionNumber() {
    assertEquals(0, CrunchJobHooks.extractPartitionNumber("out1-r-00000"));
    assertEquals(10, CrunchJobHooks.extractPartitionNumber("out2-r-00010"));
    assertEquals(99999, CrunchJobHooks.extractPartitionNumber("out3-r-99999"));
  }

  @Test
  public void testExtractPartitionNumber_WithSuffix() {
    assertEquals(10, CrunchJobHooks.extractPartitionNumber("out2-r-00010.avro"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExtractPartitionNumber_MapOutputFile() {
    CrunchJobHooks.extractPartitionNumber("out1-m-00000");
  }
}
