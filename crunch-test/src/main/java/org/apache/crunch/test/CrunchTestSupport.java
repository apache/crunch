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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.junit.Rule;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * A temporary workaround for Scala tests to use when working with Rule
 * annotations until it gets fixed in JUnit 4.11.
 */
public class CrunchTestSupport {
  @Rule
  public TemporaryPath tempDir = new TemporaryPath("crunch.tmp.dir", "hadoop.tmp.dir");

  /**
   * The method creates a {@linkplain TaskInputOutputContext} which can be used
   * in unit tests. The context serves very limited purpose. You can only
   * operate with counters, taskAttempId, status and configuration while using
   * this context.
   */
  public static <KI, VI, KO, VO> TaskInputOutputContext<KI, VI, KO, VO> getTestContext(final Configuration config) {
    TaskInputOutputContext<KI, VI, KO, VO> context = Mockito.mock(TaskInputOutputContext.class);
    TestCounters.clearCounters();
    final StateHolder holder = new StateHolder();

    Mockito.when(context.getCounter(Mockito.any(Enum.class))).then(new Answer<Counter>() {
      @Override
      public Counter answer(InvocationOnMock invocation) throws Throwable {
        Enum<?> counter = (Enum<?>) invocation.getArguments()[0];
        return TestCounters.getCounter(counter);
      }

    });

    Mockito.when(context.getCounter(Mockito.anyString(), Mockito.anyString())).then(new Answer<Counter>() {
      @Override
      public Counter answer(InvocationOnMock invocation) throws Throwable {
        String group = (String) invocation.getArguments()[0];
        String name = (String) invocation.getArguments()[1];
        return TestCounters.getCounter(group, name);
      }

    });

    Mockito.when(context.getConfiguration()).thenReturn(config);
    Mockito.when(context.getTaskAttemptID()).thenReturn(new TaskAttemptID());

    Mockito.when(context.getStatus()).then(new Answer<String>() {
      @Override
      public String answer(InvocationOnMock invocation) throws Throwable {
        return holder.internalStatus;
      }
    });

    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        holder.internalStatus = (String) invocation.getArguments()[0];
        return null;
      }
    }).when(context).setStatus(Mockito.anyString());

    return context;

  }

  static class StateHolder {
    private String internalStatus;
  }
}
