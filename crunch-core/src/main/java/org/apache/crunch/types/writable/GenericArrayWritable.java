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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * A {@link Writable} for marshalling/unmarshalling Collections. Note that
 * element order is <em>undefined</em>!
 *
 */
class GenericArrayWritable implements Writable {
  private BytesWritable[] values;
  private Class<? extends Writable> valueClass;

  public GenericArrayWritable(Class<? extends Writable> valueClass) {
    this.valueClass = valueClass;
  }

  public GenericArrayWritable() {
    // for deserialization
  }

  public void set(BytesWritable[] values) {
    this.values = values;
  }

  public BytesWritable[] get() {
    return values;
  }

  public void readFields(DataInput in) throws IOException {
    values = new BytesWritable[WritableUtils.readVInt(in)]; // construct values
    if (values.length > 0) {
      int nulls = WritableUtils.readVInt(in);
      if (nulls == values.length) {
        return;
      }
      for (int i = 0; i < values.length - nulls; i++) {
        BytesWritable value = new BytesWritable();
        value.readFields(in); // read a value
        values[i] = value; // store it in values
      }
    }
  }

  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, values.length);
    if (values.length > 0) {
      int nulls = 0;
      for (int i = 0; i < values.length; i++) {
        if (values[i] == null) {
          nulls++;
        }
      }
      WritableUtils.writeVInt(out, nulls);
      if (values.length - nulls > 0) {
        for (int i = 0; i < values.length; i++) {
          if (values[i] != null) {
            values[i].write(out);
          }
        }
      }
    }
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder();
    return hcb.append(values).toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    GenericArrayWritable other = (GenericArrayWritable) obj;
    if (!Arrays.equals(values, other.values))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return Arrays.toString(values);
  }
}
