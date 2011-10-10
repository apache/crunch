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
package com.cloudera.crunch.fn;

import com.cloudera.crunch.MapFn;

public class IdentityFn<T> extends MapFn<T, T> {

  private static final IdentityFn<Object> INSTANCE = new IdentityFn<Object>();

  @SuppressWarnings("unchecked")
  public static <T> IdentityFn<T> getInstance() {
    return (IdentityFn<T>) INSTANCE;
  }

  // Non-instantiable
  private IdentityFn() {
  }

  @Override
  public T map(T input) {
    return input;
  }
}
