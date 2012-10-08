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
package org.apache.crunch.lib.join;

import static org.mockito.Mockito.mock;

import java.util.List;

import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.crunch.test.StringWrapper;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public abstract class JoinFnTestBase {

  private JoinFn<StringWrapper, StringWrapper, String> joinFn;

  private Emitter<Pair<StringWrapper, Pair<StringWrapper, String>>> emitter;

  // Avoid warnings on generic Emitter mock
  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    joinFn = getJoinFn();
    joinFn.setConfigurationForTest(new Configuration());
    joinFn.initialize();
    emitter = mock(Emitter.class);
  }

  @Test
  public void testJoin() {

    StringWrapper key = new StringWrapper();
    StringWrapper leftValue = new StringWrapper();
    key.setValue("left-only");
    leftValue.setValue("left-only-left");
    joinFn.join(key, 0, createValuePairList(leftValue, null), emitter);

    key.setValue("both");
    leftValue.setValue("both-left");
    joinFn.join(key, 0, createValuePairList(leftValue, null), emitter);
    joinFn.join(key, 1, createValuePairList(null, "both-right"), emitter);

    key.setValue("right-only");
    joinFn.join(key, 1, createValuePairList(null, "right-only-right"), emitter);

    checkOutput(emitter);

  }

  protected abstract void checkOutput(
      Emitter<Pair<StringWrapper, Pair<StringWrapper, String>>> emitter);

  protected abstract JoinFn<StringWrapper, StringWrapper, String> getJoinFn();

  protected List<Pair<StringWrapper, String>> createValuePairList(StringWrapper leftValue,
      String rightValue) {
    Pair<StringWrapper, String> valuePair = Pair.of(leftValue, rightValue);
    List<Pair<StringWrapper, String>> valuePairList = Lists.newArrayList();
    valuePairList.add(valuePair);
    return valuePairList;
  }

}
