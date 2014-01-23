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

import com.google.common.base.Preconditions;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableUtils;

/**
 * A straight copy of the TupleWritable implementation in the join package,
 * added here because of its package visibility restrictions.
 * 
 */
public class TupleWritable extends Configured implements WritableComparable<TupleWritable> {

  private int[] written;
  private Writable[] values;

  /**
   * Create an empty tuple with no allocated storage for writables.
   */
  public TupleWritable() {
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf == null) return;

    try {
      Writables.reloadWritableComparableCodes(conf);
    } catch (Exception e) {
      throw new CrunchRuntimeException("Error reloading writable comparable codes", e);
    }
  }

  private static int[] getCodes(Writable[] writables) {
    int[] b = new int[writables.length];
    for (int i = 0; i < b.length; i++) {
      if (writables[i] != null) {
        b[i] = getCode(writables[i].getClass());
      }
    }
    return b;
  }

  public TupleWritable(Writable[] values) {
    this(values, getCodes(values));
  }

  /**
   * Initialize tuple with storage; unknown whether any of them contain
   * &quot;written&quot; values.
   */
  public TupleWritable(Writable[] values, int[] written) {
    Preconditions.checkArgument(values.length == written.length);
    this.written = written;
    this.values = values;
  }

  /**
   * Return true if tuple has an element at the position provided.
   */
  public boolean has(int i) {
    return written[i] != 0;
  }

  /**
   * Get ith Writable from Tuple.
   */
  public Writable get(int i) {
    return values[i];
  }

  /**
   * The number of children in this Tuple.
   */
  public int size() {
    return values.length;
  }

  /**
   * {@inheritDoc}
   */
  public boolean equals(Object other) {
    if (other instanceof TupleWritable) {
      TupleWritable that = (TupleWritable) other;
      if (this.size() != that.size()) {
        return false;
      }
      for (int i = 0; i < values.length; ++i) {
        if (!has(i))
          continue;
        if (written[i] != that.written[i] || !values[i].equals(that.values[i])) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();
    builder.append(written);
    for (Writable v : values) {
      builder.append(v);
    }
    return builder.toHashCode();
  }

  /**
   * Convert Tuple to String as in the following.
   * <tt>[<child1>,<child2>,...,<childn>]</tt>
   */
  public String toString() {
    StringBuffer buf = new StringBuffer("[");
    for (int i = 0; i < values.length; ++i) {
      if (has(i)) {
        buf.append(values[i].toString());
      }
      buf.append(",");
    }
    if (values.length != 0)
      buf.setCharAt(buf.length() - 1, ']');
    else
      buf.append(']');
    return buf.toString();
  }

  public void clear() {
    Arrays.fill(written, (byte) 0);
  }

  public void set(int index, Writable w) {
    written[index] = getCode(w.getClass());
    values[index] = w;
  }

  /**
   * Writes each Writable to <code>out</code>. TupleWritable format:
   * {@code
   *  <count><type1><type2>...<typen><obj1><obj2>...<objn>
   * }
   */
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, values.length);
    for (int i = 0; i < values.length; ++i) {
      WritableUtils.writeVInt(out, written[i]);
      if (written[i] != 0) {
        values[i].write(out);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  public void readFields(DataInput in) throws IOException {
    int card = WritableUtils.readVInt(in);
    values = new Writable[card];
    written = new int[card];
    for (int i = 0; i < card; ++i) {
      written[i] = WritableUtils.readVInt(in);
      if (written[i] != 0) {
        values[i] = getWritable(written[i], getConf());
        values[i].readFields(in);
      }
    }
  }

  static int getCode(Class<? extends Writable> clazz) {
    if (Writables.WRITABLE_CODES.inverse().containsKey(clazz)) {
      return Writables.WRITABLE_CODES.inverse().get(clazz);
    } else {
      return 1; // default for BytesWritable
    }
  }

  static Writable getWritable(int code, Configuration conf) {
    Class<? extends Writable> clazz = Writables.WRITABLE_CODES.get(code);
    if (clazz != null) {
      return WritableFactories.newInstance(clazz, conf);
    } else {
      throw new IllegalStateException("Unknown Writable code: " + code);
    }
  }

  @Override
  public int compareTo(TupleWritable o) {
    for (int i = 0; i < values.length; ++i) {
      if (has(i) && !o.has(i)) {
        return 1;
      } else if (!has(i) && o.has(i)) {
        return -1;
      } else {
        Writable v1 = values[i];
        Writable v2 = o.values[i];
        if (v1 != v2 && (v1 != null && !v1.equals(v2))) {
          if (v1 instanceof WritableComparable && v2 instanceof WritableComparable) {
            int cmp = ((WritableComparable) v1).compareTo((WritableComparable) v2);
            if (cmp != 0) {
              return cmp;
            }
          } else {
            int cmp = v1.hashCode() - v2.hashCode();
            if (cmp != 0) {
              return cmp;
            }
          }
        }
      }
    }
    return values.length - o.values.length;
  }
}