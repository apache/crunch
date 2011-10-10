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
package com.cloudera.crunch.impl.mr.emit;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.cloudera.crunch.CombineFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.impl.mr.run.RTNode;
import com.google.common.collect.ImmutableList;

public class CombineFnEmitter implements Emitter<Object> {

  private final CombineFn<Object, Object> combineFn;
  private final RTNode delegate;
  private final int cacheSize;
  private final Map<Object, Object> cache = new LinkedHashMap<Object, Object>() {
    protected boolean removeEldestEntry(Map.Entry<Object, Object> eldest) {
      if (size() > cacheSize) {
        delegate.process(Pair.of(eldest.getKey(), eldest.getValue()));
        return true;
      }
      return false;
    }
  };

  public CombineFnEmitter(CombineFn<Object, Object> combineFn, RTNode delegate,
      int cacheSize) {
    this.combineFn = combineFn;
    this.delegate = delegate;
    this.cacheSize = cacheSize;
  }

  public void emit(Object emitted) {
    Pair<Object, Object> e = (Pair<Object, Object>) emitted;
    if (cache.containsKey(e.first())) {
      List<Object> values = ImmutableList.of(cache.get(e.first()), e.second());
      cache.put(e.first(), combineFn.combine(values));
    } else {
      cache.put(e.first(), combineFn.combine(ImmutableList.of(e.second())));
    }
  }

  public void flush() {
    for (Map.Entry<Object, Object> entry : cache.entrySet()) {
      delegate.process(Pair.of(entry.getKey(), entry.getValue()));
    }
  }
}
