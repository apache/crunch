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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.crunch.PCollection;

/**
 * A concrete implementation of {@link org.apache.crunch.materialize.pobject.PObjectImpl} whose
 * value is a Java {@link java.util.Collection} containing the elements of the underlying {@link
 * PCollection} for this {@link org.apache.crunch.PObject}.
 *
 * @param <S> The value type for elements contained in the {@code Collection} value encapsulated
 * by this {@code PObject}.
 */
public class CollectionPObject<S> extends PObjectImpl<S, Collection<S>> {

  /**
   * Constructs a new instance of this {@code PObject} implementation.
   *
   * @param collect The backing {@code PCollection} for this {@code PObject}.
   */
  public CollectionPObject(PCollection<S> collect) {
    super(collect);
  }

  /** {@inheritDoc} */
  @Override
  public Collection<S> process(Iterable<S> input) {
    Collection<S> target = new ArrayList<S>();
    Iterator<S> itr = input.iterator();
    while (itr.hasNext()) {
      target.add(itr.next());
    }
    return target;
  }
}
