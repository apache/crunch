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

import static org.junit.Assert.assertTrue;

import org.apache.crunch.Pair;
import org.apache.crunch.types.PTypeFamily;

public class RightOuterJoinIT extends JoinTester {
  @Override
  public void assertPassed(Iterable<Pair<String, Long>> lines) {
    boolean passed1 = false;
    boolean passed2 = true;
    boolean passed3 = false;
    for (Pair<String, Long> line : lines) {
      if ("wretched".equals(line.first()) && 24 == line.second()) {
        passed1 = true;
      }
      if ("againe".equals(line.first())) {
        passed2 = false;
      }
      if ("Montparnasse.".equals(line.first()) && 2 == line.second()) {
        passed3 = true;
      }
    }
    assertTrue(passed1);
    assertTrue(passed2);
    assertTrue(passed3);
  }

  @Override
  protected JoinFn<String, Long, Long> getJoinFn(PTypeFamily typeFamily) {
    return new RightOuterJoinFn<String, Long, Long>(typeFamily.strings(), typeFamily.longs());
  }
}
