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

import java.io.IOException;

import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.impl.mr.run.CrunchRuntimeException;
import com.cloudera.crunch.types.Converter;

public class OutputEmitter<T, K, V> implements Emitter<T> {

  private final Converter<K, V, Object, Object> converter;
  private final TaskInputOutputContext<?, ?, K, V> context;

  public OutputEmitter(Converter<K, V, Object, Object> converter,
      TaskInputOutputContext<?, ?, K, V> context) {
    this.converter = converter;
    this.context = context;
  }

  public void emit(T emitted) {
    try {
      K key = converter.outputKey(emitted);
      V value = converter.outputValue(emitted);
      this.context.write(key, value);
    } catch (IOException e) {
      throw new CrunchRuntimeException(e);
    } catch (InterruptedException e) {
      throw new CrunchRuntimeException(e);
    }
  }

  public void flush() {
    // No-op
  }
}
