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
package org.apache.crunch.impl.mr.emit;

import java.io.IOException;

import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.Emitter;
import org.apache.crunch.types.Converter;
import org.apache.crunch.hadoop.mapreduce.lib.output.CrunchMultipleOutputs;

public class MultipleOutputEmitter<T, K, V> implements Emitter<T> {

  private final Converter converter;
  private final CrunchMultipleOutputs<K, V> outputs;
  private final String outputName;

  public MultipleOutputEmitter(Converter converter, CrunchMultipleOutputs<K, V> outputs, String outputName) {
    this.converter = converter;
    this.outputs = outputs;
    this.outputName = outputName;
  }

  @Override
  public void emit(T emitted) {
    try {
      this.outputs.write(outputName, converter.outputKey(emitted), converter.outputValue(emitted));
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
