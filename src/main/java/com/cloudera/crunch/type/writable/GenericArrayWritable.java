package com.cloudera.crunch.type.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

import com.cloudera.crunch.impl.mr.run.CrunchRuntimeException;

public class GenericArrayWritable implements Writable {
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
    values = new Writable[in.readInt()];          // construct values
    if(values.length > 0) {
      String valueType = Text.readString(in);
      try {
        valueClass = Class.forName(valueType).asSubclass(Writable.class);      
      } catch (ClassNotFoundException e) {
        throw new CrunchRuntimeException(e);
      }
      for (int i = 0; i < values.length; i++) {
        Writable value = WritableFactories.newInstance(valueClass);
        value.readFields(in);                       // read a value
        values[i] = value;                          // store it in values
      }
    }
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(values.length);                 // write values;
    if(values.length > 0) {
      if(valueClass == null) {
        valueClass = values[0].getClass();
      }
      Text.writeString(out, valueClass.getName());
      for (int i = 0; i < values.length; i++) {
        values[i].write(out);
      }
    }
  }
  @Override
  public String toString() {
    return "GenericArrayWritable [values=" + Arrays.toString(values)
        + ", valueClass=" + valueClass + "]";
  }
}
