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
package com.cloudera.crunch;

import com.cloudera.crunch.type.PTableType;
import com.cloudera.crunch.type.PType;

/**
 * The base class for {@code Source} implementations that return a {@link PTable}.
 *
 */
public abstract class TableSource<K, V> implements Source<Pair<K, V>> {
  @Override
  public PType<Pair<K, V>> getType() {
    return getTableType();
  }
  
  public abstract PTableType<K, V> getTableType();
}
