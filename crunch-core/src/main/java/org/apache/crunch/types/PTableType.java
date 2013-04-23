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
package org.apache.crunch.types;

import org.apache.crunch.PTable;
import org.apache.crunch.Pair;

/**
 * An extension of {@code PType} specifically for {@link PTable} objects. It
 * allows separate access to the {@code PType}s of the key and value for the
 * {@code PTable}.
 * 
 */
public interface PTableType<K, V> extends PType<Pair<K, V>> {
  /**
   * Returns the key type for the table.
   */
  PType<K> getKeyType();

  /**
   * Returns the value type for the table.
   */
  PType<V> getValueType();

  /**
   * Returns the grouped table version of this type.
   */
  PGroupedTableType<K, V> getGroupedTableType();
}
