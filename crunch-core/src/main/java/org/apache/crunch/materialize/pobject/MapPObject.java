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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;

/**
 * A concrete implementation of {@link PObjectImpl} whose
 * value is a Java {@link Map}. The underlying {@link PCollection} for this
 * {@link org.apache.crunch.PObject} must contain {@link Pair}s of values. The
 * first element of the pair will be used as the map key, while the second element will be used
 * as the map value.  Note that the contents of the underlying {@code PCollection} may not be
 * reflected in the returned {@code Map}, since a single key may be mapped to several values in
 * the underlying {@code PCollection}, and only one of those values will appear in the {@code
 * Map} encapsulated by this {@code PObject}.
 *
 * @param <K> The type of keys for the Map.
 * @param <V> The type of values for the Map.
 */
public class MapPObject<K, V> extends PObjectImpl<Pair<K, V>, Map<K, V>> {

  /**
   * Constructs a new instance of this {@code PObject} implementation.
   *
   * @param collect The backing {@code PCollection} for this {@code PObject}.
   */
  public MapPObject(PCollection<Pair<K, V>> collect) {
    super(collect);
  }

  /** {@inheritDoc} */
  @Override
  public Map<K, V> process(Iterable<Pair<K, V>> input) {
    Map<K, V> target = new HashMap<K, V>();
    Iterator<Pair<K, V>> itr = input.iterator();
    while (itr.hasNext()) {
      Pair<K, V> pair = itr.next();
      target.put(pair.first(), pair.second());
    }
    return target;
  }
}
