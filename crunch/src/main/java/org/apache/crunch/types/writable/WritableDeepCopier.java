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
package org.apache.crunch.types.writable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.apache.crunch.impl.mr.run.CrunchRuntimeException;
import org.apache.crunch.types.DeepCopier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

/**
 * Performs deep copies of Writable values.
 * 
 * @param <T> The type of Writable that can be copied
 */
public class WritableDeepCopier<T extends Writable> implements DeepCopier<T> {

  private Class<T> writableClass;

  public WritableDeepCopier(Class<T> writableClass) {
    this.writableClass = writableClass;
  }

  @Override
  public void initialize(Configuration conf) {
  }

  @Override
  public T deepCopy(T source) {
    ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(byteOutStream);
    T copiedValue = null;
    try {
      source.write(dataOut);
      dataOut.flush();
      ByteArrayInputStream byteInStream = new ByteArrayInputStream(byteOutStream.toByteArray());
      DataInput dataInput = new DataInputStream(byteInStream);
      copiedValue = writableClass.newInstance();
      copiedValue.readFields(dataInput);
    } catch (Exception e) {
      throw new CrunchRuntimeException("Error while deep copying " + source, e);
    }
    return copiedValue;
  }
}
