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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.crunch.test.StringWrapper;
import org.apache.crunch.types.avro.Avros;

public class LeftOuterJoinTest extends JoinFnTestBase {

  @Override
  protected void checkOutput(Emitter<Pair<StringWrapper, Pair<StringWrapper, String>>> emitter) {
    verify(emitter)
        .emit(Pair.of(wrap("left-only"), Pair.of(wrap("left-only-left"), (String) null)));
    verify(emitter).emit(Pair.of(wrap("both"), Pair.of(wrap("both-left"), "both-right")));
    verifyNoMoreInteractions(emitter);
  }

  @Override
  protected JoinFn<StringWrapper, StringWrapper, String> getJoinFn() {
    return new LeftOuterJoinFn<StringWrapper, StringWrapper, String>(
        Avros.reflects(StringWrapper.class),
        Avros.reflects(StringWrapper.class));
  }

}
