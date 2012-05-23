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
package com.cloudera.crunch.types.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableUtils;

import com.cloudera.crunch.impl.mr.run.CrunchRuntimeException;

public class GenericArrayWritable<T> implements Writable {
  private Writable[] values;
  private Class<? extends Writable> valueClass;

  public GenericArrayWritable(Class<? extends Writable> valueClass) {
    this.valueClass = valueClass;
  }
  
  public GenericArrayWritable() {
    // for deserialization
  }
  
  public void set(Writable[] values) { 
    this.values = values; 
  }

  public Writable[] get() {
    return values;
  }

  public void readFields(DataInput in) throws IOException {
    values = new Writable[WritableUtils.readVInt(in)];          // construct values
    if (values.length > 0) {
      int nulls = WritableUtils.readVInt(in);
      if (nulls == values.length) {
        return;
      }
      String valueType = Text.readString(in);
      setValueType(valueType);
      for (int i = 0; i < values.length; i++) {
        Writable value = WritableFactories.newInstance(valueClass);
        value.readFields(in);                       // read a value
        values[i] = value;                          // store it in values
      }
    }
  }
  
  protected void setValueType(String valueType) {
    if (valueClass == null) {
      try {
        valueClass = Class.forName(valueType).asSubclass(Writable.class);      
      } catch (ClassNotFoundException e) {
        throw new CrunchRuntimeException(e);
      }
    } else if (!valueType.equals(valueClass.getName()))  {
      throw new IllegalStateException("Incoming " + valueType + " is not " + valueClass);
    }
  }
  
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, values.length);
    int nulls = 0;
    for (int i = 0; i < values.length; i++) {
      if (values[i] == null) {
        nulls++;
      }
    }
    WritableUtils.writeVInt(out, nulls);
    if (values.length - nulls > 0) {
      if (valueClass == null) {
        throw new IllegalStateException("Value class not set by constructor or read");
      }
      Text.writeString(out, valueClass.getName());
      for (int i = 0; i < values.length; i++) {
        if (values[i] != null) {
          values[i].write(out);
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
