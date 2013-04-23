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
package org.apache.crunch.materialize.pobject;

import java.util.Iterator;

import org.apache.crunch.PCollection;

/**
 * A concrete implementation of {@link PObjectImpl} that uses the first element in the backing
 * {@link PCollection} as the {@link org.apache.crunch.PObject} value.
 *
 * @param <T> The value type of this {@code PObject}.
 */
public class FirstElementPObject<T> extends PObjectImpl<T, T> {

  /**
   * Constructs a new instance of this {@code PObject} implementation.
   *
   * @param collect The backing {@code PCollection} for this {@code PObject}.
   */
  public FirstElementPObject(PCollection<T> collect) {
    super(collect);
  }

  /** {@inheritDoc} */
  @Override
  public T process(Iterable<T> input) {
    Iterator<T> itr = input.iterator();
    if (itr.hasNext()) {
      return itr.next();
    }
    return null;
  }
}
