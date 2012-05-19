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

import org.apache.hadoop.mapreduce.lib.output.CrunchMultipleOutputs;

import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.impl.mr.run.CrunchRuntimeException;
import com.cloudera.crunch.types.Converter;

public class MultipleOutputEmitter<T, K, V> implements Emitter<T> {

  private final Converter converter;
  private final CrunchMultipleOutputs<K, V> outputs;
  private final String outputName;

  public MultipleOutputEmitter(Converter converter,
      CrunchMultipleOutputs<K, V> outputs, String outputName) {
    this.converter = converter;
    this.outputs = outputs;
    this.outputName = outputName;
  }

  @Override
  public void emit(T emitted) {
    try {
      this.outputs.write(outputName, converter.outputKey(emitted),
          converter.outputValue(emitted));
    } catch (IOException e) {
      throw new CrunchRuntimeException(e);
    } catch (InterruptedException e) {
      throw new CrunchRuntimeException(e);
    }
  }

  @Override
  public void flush() {
    // No-op
  }

}
