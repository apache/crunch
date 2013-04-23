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
package org.apache.crunch.impl.mr.emit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.apache.crunch.impl.mr.run.RTNode;
import org.apache.crunch.test.StringWrapper;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.google.common.collect.Lists;

public class IntermediateEmitterTest {

  private StringWrapper stringWrapper;
  private PType ptype;

  @Before
  public void setUp() {
    stringWrapper = new StringWrapper("test");
    ptype = spy(Avros.reflects(StringWrapper.class));
  }

  @Test
  public void testEmit_SingleChild() {
    RTNode singleChild = mock(RTNode.class);
    IntermediateEmitter emitter = new IntermediateEmitter(ptype, Lists.newArrayList(singleChild),
        new Configuration());
    emitter.emit(stringWrapper);

    ArgumentCaptor<StringWrapper> argumentCaptor = ArgumentCaptor.forClass(StringWrapper.class);
    verify(singleChild).process(argumentCaptor.capture());
    assertSame(stringWrapper, argumentCaptor.getValue());
  }

  @Test
  public void testEmit_MultipleChildren() {
    RTNode childA = mock(RTNode.class);
    RTNode childB = mock(RTNode.class);
    IntermediateEmitter emitter = new IntermediateEmitter(ptype, Lists.newArrayList(childA, childB),
        new Configuration());
    emitter.emit(stringWrapper);

    ArgumentCaptor<StringWrapper> argumentCaptorA = ArgumentCaptor.forClass(StringWrapper.class);
    ArgumentCaptor<StringWrapper> argumentCaptorB = ArgumentCaptor.forClass(StringWrapper.class);

    verify(childA).process(argumentCaptorA.capture());
    verify(childB).process(argumentCaptorB.capture());

    assertEquals(stringWrapper, argumentCaptorA.getValue());
    assertEquals(stringWrapper, argumentCaptorB.getValue());

    // Make sure that multiple children means deep copies are performed
    assertNotSame(stringWrapper, argumentCaptorA.getValue());
    assertNotSame(stringWrapper, argumentCaptorB.getValue());
  }

}
