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
package org.apache.crunch.impl;

import java.util.Iterator;

/**
 * Wrapper around a Reducer's input Iterable. Ensures that the
 * {@link #iterator()} method is not called more than once.
 */
public class SingleUseIterable<T> implements Iterable<T> {

  private boolean used = false;
  private Iterable<T> wrappedIterable;

  /**
   * Instantiate around an Iterable that may only be used once.
   * 
   * @param toWrap iterable to wrap
   */
  public SingleUseIterable(Iterable<T> toWrap) {
    this.wrappedIterable = toWrap;
  }

  @Override
  public Iterator<T> iterator() {
    if (used) {
      throw new IllegalStateException("iterator() can only be called once on this Iterable");
    }
    used = true;
    return wrappedIterable.iterator();
  }

}
