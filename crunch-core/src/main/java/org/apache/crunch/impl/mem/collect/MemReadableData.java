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
package org.apache.crunch.impl.mem.collect;

import com.google.common.collect.ImmutableSet;
import org.apache.crunch.ReadableData;
import org.apache.crunch.SourceTarget;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

class MemReadableData<T> implements ReadableData<T> {

  private Collection<T> collection;

  public MemReadableData(Collection<T> collection) {
    this.collection = collection;
  }

  @Override
  public Set<SourceTarget<?>> getSourceTargets() {
    return ImmutableSet.of();
  }

  @Override
  public void configure(Configuration conf) {
    // No-op
  }

  @Override
  public Iterable<T> read(TaskInputOutputContext<?, ?, ?, ?> ctxt) throws IOException {
    return collection;
  }
}
