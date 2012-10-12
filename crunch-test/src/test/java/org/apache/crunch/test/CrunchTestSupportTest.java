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
package org.apache.crunch.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.junit.Test;

public class CrunchTestSupportTest {

  enum CT {
    ONE,
  };

  @Test
  public void testContext() {
    Configuration sampleConfig = new Configuration();
    String status = "test";
    TaskInputOutputContext<?, ?, ?, ?> testContext = CrunchTestSupport.getTestContext(sampleConfig);
    assertEquals(sampleConfig, testContext.getConfiguration());
    TaskAttemptID taskAttemptID = testContext.getTaskAttemptID();
    assertEquals(taskAttemptID, testContext.getTaskAttemptID());
    assertNotNull(taskAttemptID);
    assertNull(testContext.getStatus());
    testContext.setStatus(status);
    assertEquals(status, testContext.getStatus());
    assertEquals(0, testContext.getCounter(CT.ONE).getValue());
    testContext.getCounter(CT.ONE).increment(1);
    assertEquals(1, testContext.getCounter(CT.ONE).getValue());
    testContext.getCounter(CT.ONE).increment(4);
    assertEquals(5, testContext.getCounter(CT.ONE).getValue());
  }
}
