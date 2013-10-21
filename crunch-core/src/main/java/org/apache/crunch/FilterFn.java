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
package org.apache.crunch;

import java.util.List;

import org.apache.crunch.fn.FilterFns;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.google.common.collect.ImmutableList;

/**
 * A {@link DoFn} for the common case of filtering the members of a
 * {@link PCollection} based on a boolean condition.
 */
public abstract class FilterFn<T> extends DoFn<T, T> {

  /**
   * If true, emit the given record.
   */
  public abstract boolean accept(T input);

  @Override
  public void process(T input, Emitter<T> emitter) {
    if (accept(input)) {
      emitter.emit(input);
    }
  }
  
  @Override
  public final void cleanup(Emitter<T> emitter) {
    cleanup();
  }
  
  /**
   * Called during the cleanup of the MapReduce job this {@code FilterFn} is
   * associated with. Subclasses may override this method to do appropriate
   * cleanup.
   */
  public void cleanup() {
  }
  
  @Override
  public float scaleFactor() {
    return 0.5f;
  }
}
