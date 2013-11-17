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
package org.apache.crunch.util;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.crunch.ReadableData;
import org.apache.crunch.SourceTarget;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class UnionReadableData<T> implements ReadableData<T> {

  private final List<ReadableData<T>> data;

  public UnionReadableData(List<ReadableData<T>> data) {
    this.data = data;
  }

  @Override
  public Set<SourceTarget<?>> getSourceTargets() {
    Set<SourceTarget<?>> srcTargets = Sets.newHashSet();
    for (ReadableData<T> rd: data) {
      srcTargets.addAll(rd.getSourceTargets());
    }
    return srcTargets;
  }

  @Override
  public void configure(Configuration conf) {
   for (ReadableData<T> rd : data) {
     rd.configure(conf);
   }
  }

  @Override
  public Iterable<T> read(final TaskInputOutputContext<?, ?, ?, ?> context) throws IOException {
    List<Iterable<T>> iterables = Lists.newArrayList();
    for (ReadableData<T> rd : data) {
      iterables.add(rd.read(context));
    }
    return Iterables.concat(iterables);
  }
}
