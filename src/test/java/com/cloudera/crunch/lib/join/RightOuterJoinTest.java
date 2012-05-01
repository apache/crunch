/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.crunch.lib.join;

import static org.junit.Assert.assertTrue;

import com.cloudera.crunch.Pair;

public class RightOuterJoinTest extends JoinTester {
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
  protected JoinFn<String, Long, Long> getJoinFn() {
    return new RightOuterJoinFn<String, Long, Long>();
  }
}
