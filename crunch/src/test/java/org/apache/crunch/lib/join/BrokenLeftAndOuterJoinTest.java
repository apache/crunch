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

import static org.apache.crunch.test.StringWrapper.wrap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.List;

import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.crunch.test.CrunchTestSupport;
import org.apache.crunch.test.StringWrapper;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.google.common.collect.Lists;

public class BrokenLeftAndOuterJoinTest {

  List<Pair<StringWrapper, String>> createValuePairList(StringWrapper leftValue, String rightValue) {
    Pair<StringWrapper, String> valuePair = Pair.of(leftValue, rightValue);
    List<Pair<StringWrapper, String>> valuePairList = Lists.newArrayList();
    valuePairList.add(valuePair);
    return valuePairList;
  }
  
  @Test
  public void testOuterJoin() {
    JoinFn<StringWrapper, StringWrapper, String> joinFn = new LeftOuterJoinFn<StringWrapper, StringWrapper, String>(
        Avros.reflects(StringWrapper.class),
        Avros.reflects(StringWrapper.class));
    joinFn.setContext(CrunchTestSupport.getTestContext(new Configuration()));
    joinFn.initialize();
    Emitter<Pair<StringWrapper, Pair<StringWrapper, String>>> emitter = mock(Emitter.class);
    
    StringWrapper key = new StringWrapper();
    StringWrapper leftValue = new StringWrapper();
    key.setValue("left-only");
    leftValue.setValue("left-only-left");
    joinFn.join(key, 0, createValuePairList(leftValue, null), emitter);

    key.setValue("right-only");
    joinFn.join(key, 1, createValuePairList(null, "right-only-right"), emitter);

    verify(emitter).emit(Pair.of(wrap("left-only"), Pair.of(wrap("left-only-left"), (String) null)));
    verifyNoMoreInteractions(emitter);
  }
  
  @Test
  public void testFullJoin() {
    JoinFn<StringWrapper, StringWrapper, String> joinFn = new FullOuterJoinFn<StringWrapper, StringWrapper, String>(
        Avros.reflects(StringWrapper.class),
        Avros.reflects(StringWrapper.class));
    joinFn.setContext(CrunchTestSupport.getTestContext(new Configuration()));
    joinFn.initialize();
    Emitter<Pair<StringWrapper, Pair<StringWrapper, String>>> emitter = mock(Emitter.class);
    
    StringWrapper key = new StringWrapper();
    StringWrapper leftValue = new StringWrapper();
    key.setValue("left-only");
    leftValue.setValue("left-only-left");
    joinFn.join(key, 0, createValuePairList(leftValue, null), emitter);

    key.setValue("right-only");
    joinFn.join(key, 1, createValuePairList(null, "right-only-right"), emitter);

    verify(emitter).emit(Pair.of(wrap("left-only"), Pair.of(wrap("left-only-left"), (String) null)));
    verify(emitter).emit(Pair.of(wrap("right-only"), Pair.of((StringWrapper)null, "right-only-right")));
    verifyNoMoreInteractions(emitter);
  }
}
