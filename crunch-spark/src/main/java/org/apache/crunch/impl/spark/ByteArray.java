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
package org.apache.crunch.impl.spark;

import java.io.Serializable;

public class ByteArray implements Serializable, Comparable<ByteArray> {

  public final byte[] value;
  protected final ByteArrayHelper helper;

  public ByteArray(byte[] value, ByteArrayHelper helper) {
    this.value = value;
    this.helper = helper;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    ByteArray byteArray = (ByteArray) o;
    return helper.equal(value, byteArray.value);
  }

  @Override
  public int hashCode() {
    return helper.hashCode(value);
  }

  @Override
  public int compareTo(ByteArray other) {
    return helper.compare(value, other.value);
  }
}
