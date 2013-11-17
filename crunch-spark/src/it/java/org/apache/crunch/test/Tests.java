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

import static com.google.common.base.Preconditions.checkNotNull;

public class Tests {
  /**
   * This doesn't check whether the resource exists!
   *
   * @param testCase
   * @param resourceName
   * @return The path to the resource (never null)
   */
  public static String resource(Object testCase, String resourceName) {
    checkNotNull(testCase);
    checkNotNull(resourceName);

    // Note: We append "Data" because otherwise Eclipse would complain about the
    //       the case's class name clashing with the resource directory's name.
    return testCase.getClass().getName().replaceAll("\\.", "/") + "Data/" + resourceName;
  }
}
