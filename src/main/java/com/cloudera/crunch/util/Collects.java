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
package com.cloudera.crunch.util;

import java.util.Collection;
import java.util.Iterator;

import com.google.common.collect.Lists;

/**
 * Utility functions for returning Collection objects backed by different types
 * of implementations.
 */
public class Collects {
  
  public static <T> Collection<T> newArrayList() {
    return Lists.newArrayList();
  }

  public static <T> Collection<T> newArrayList(T...elements) {
    return Lists.newArrayList(elements);
  }

  public static <T> Collection<T> newArrayList(Iterable<? extends T> elements) {
    return Lists.newArrayList(elements);
  }

  public static <T> Collection<T> newArrayList(Iterator<? extends T> elements) {
    return Lists.newArrayList(elements);
  }

  private Collects() {}
}
