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
package org.apache.crunch.impl.spark.serde;

import com.google.common.base.Function;
import org.apache.crunch.impl.spark.ByteArray;
import org.apache.crunch.impl.spark.ByteArrayHelper;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class WritableSerDe implements SerDe<Writable> {

  Class<? extends Writable> clazz;

  public WritableSerDe(Class<? extends Writable> clazz) {
    this.clazz = clazz;
  }

  @Override
  public ByteArray toBytes(Writable obj) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    obj.write(dos);
    dos.close();
    return new ByteArray(baos.toByteArray(), ByteArrayHelper.WRITABLES);
  }

  @Override
  public Writable fromBytes(byte[] bytes) {
    Writable inst = ReflectionUtils.newInstance(clazz, null);
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    DataInputStream dis = new DataInputStream(bais);
    try {
      inst.readFields(dis);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return inst;
  }

  @Override
  public Function<byte[], Writable> fromBytesFunction() {
    return new Function<byte[], Writable>() {
      @Override
      public Writable apply(@Nullable byte[] input) {
        return fromBytes(input);
      }
    };
  }
}
