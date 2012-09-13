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

import org.apache.crunch.PCollection;
import org.apache.crunch.PObject;
import org.apache.crunch.Pipeline;
import org.apache.crunch.Target;

/**
 * An abstract implementation of {@link PObject} that is backed by a {@link PCollection}.
 * Clients creating a concrete implementation should override the method
 * {@link PObjectImpl#process(Iterable)}, which transforms the backing PCollection into the
 * singleton value encapsulated by the PObject. Once this {code PObject}'s value has been
 * calculated, the value is cached to prevent subsequent materializations of the backing
 * {@code PCollection}.
 *
 * @param <S> The type contained in the underlying PCollection.
 * @param <T> The type encapsulated by this PObject.
 */
public abstract class PObjectImpl<S, T> implements PObject<T> {

  // The underlying PCollection whose contents will be used to generate the value for this
  // PObject.
  private PCollection<S> collection;

  // A variable to hold a cached copy of the value of this {@code PObject},
  // to prevent unnecessary materializations of the backing {@code PCollection}.
  private T cachedValue;

  // A flag indicating if a value for this {@code PObject} has been cached.
  private boolean isCached;

  /**
   * Constructs a new instance of this {@code PObject} implementation.
   *
   * @param collect The backing {@code PCollection} for this {@code PObject}.
   */
  public PObjectImpl(PCollection<S> collect) {
    this.collection = collect;
    this.cachedValue = null;
    this.isCached = false;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return collection.toString();
  }

  /** {@inheritDoc} */
  @Override
  public final T getValue() {
    if (!isCached) {
      cachedValue = process(collection.materialize());
      isCached = true;
    }
    return cachedValue;
  }

  /**
   * Transforms the provided Iterable, obtained from the backing {@link PCollection},
   * into the value encapsulated by this {@code PObject}.
   *
   * @param input An Iterable whose elements correspond to those of the backing {@code
   * PCollection}.
   * @return The value of this {@code PObject}.
   */
  protected abstract T process(Iterable<S> input);
}
