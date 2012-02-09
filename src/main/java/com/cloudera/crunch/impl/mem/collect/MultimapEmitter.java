/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
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
package com.cloudera.crunch.impl.mem.collect;

import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.Pair;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

class MultimapEmitter<K, V> implements Emitter<Pair<K, V>> {

  private final Multimap<K, V> collect = HashMultimap.create();
  
  @Override
  public void emit(Pair<K, V> emitted) {
    collect.put(emitted.first(), emitted.second());
  }

  @Override
  public void flush() {    
  }
  
  public Multimap<K, V> getOutput() {
    return collect;
  }
}
