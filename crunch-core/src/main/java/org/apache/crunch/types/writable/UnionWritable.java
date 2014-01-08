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

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UnionWritable implements WritableComparable<UnionWritable> {

  private int index;
  private BytesWritable value;

  public UnionWritable() {
    // no-arg constructor for writables
  }

  public UnionWritable(int index, BytesWritable value) {
    this.index = index;
    this.value = value;
  }

  public int getIndex() {
    return index;
  }

  public BytesWritable getValue() {
    return value;
  }

  @Override
  public int compareTo(UnionWritable other) {
    if (index == other.getIndex()) {
      return value.compareTo(other.getValue());
    }
    return index - other.getIndex();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, index);
    value.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.index = WritableUtils.readVInt(in);
    if (value == null) {
      value = new BytesWritable();
    }
    value.readFields(in);
  }
}
