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
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A serialization format for {@link org.apache.crunch.Tuple}.
 *
 * <pre>
 *   tuple_writable ::= card field+
 *   card ::= vint
 *   field ::= code [body_size body]
 *   code ::= vint
 *   body_size ::= vint
 *   body ::= byte[]
 * </pre>
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
   * Writes each Writable to <code>out</code>.
   */
  public void write(DataOutput out) throws IOException {
    DataOutputBuffer tmp = new DataOutputBuffer();
    WritableUtils.writeVInt(out, values.length);
    for (int i = 0; i < values.length; ++i) {
      WritableUtils.writeVInt(out, written[i]);
      if (written[i] != 0) {
        tmp.reset();
        values[i].write(tmp);
        WritableUtils.writeVInt(out, tmp.getLength());
        out.write(tmp.getData(), 0, tmp.getLength());
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
        WritableUtils.readVInt(in); // skip "bodySize"
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
  public int compareTo(TupleWritable that) {
    for (int i = 0; i < Math.min(this.size(), that.size()); i++) {
      if (!this.has(i) && !that.has(i)) {
        continue;
      }
      if (this.has(i) && !that.has(i)) {
        return 1;
      }
      if (!this.has(i) && that.has(i)) {
        return -1;
      }
      if (this.written[i] != that.written[i]) {
        return this.written[i] - that.written[i];
      }
      Writable v1 = this.values[i];
      Writable v2 = that.values[i];
      int cmp;
      if (v1 instanceof WritableComparable && v2 instanceof WritableComparable) {
        cmp = ((WritableComparable) v1).compareTo(v2);
      } else {
        cmp = v1.hashCode() - v2.hashCode();
      }
      if (cmp != 0) {
        return cmp;
      }
    }
    return this.size() - that.size();
  }

  public static class Comparator extends WritableComparator implements Configurable {

    private static final Comparator INSTANCE = new Comparator();

    public static Comparator getInstance() {
      return INSTANCE;
    }

    public Comparator() {
      super(TupleWritable.class);
    }

    @Override
    public void setConf(Configuration conf) {
      if (conf == null) return;
      try {
        Writables.reloadWritableComparableCodes(conf);
      } catch (Exception e) {
        throw new CrunchRuntimeException("Error reloading writable comparable codes", e);
      }
    }

    @Override
    public Configuration getConf() {
      return null;
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      DataInputBuffer buffer1 = new DataInputBuffer();
      DataInputBuffer buffer2 = new DataInputBuffer();

      try {
        buffer1.reset(b1, s1, l1);
        buffer2.reset(b2, s2, l2);

        int card1 = WritableUtils.readVInt(buffer1);
        int card2 = WritableUtils.readVInt(buffer2);
        int minCard = Math.min(card1, card2);

        for (int i = 0; i < minCard; i++) {
          int cmp = compareField(buffer1, buffer2);
          if (cmp != 0) {
            return cmp;
          }
        }
        return card1 - card2;
      } catch (IOException e) {
        throw new CrunchRuntimeException(e);
      }
    }

    private int compareField(DataInputBuffer buffer1, DataInputBuffer buffer2) throws IOException {
      int written1 = WritableUtils.readVInt(buffer1);
      int written2 = WritableUtils.readVInt(buffer2);
      boolean hasValue1 = (written1 != 0);
      boolean hasValue2 = (written2 != 0);
      if (!hasValue1 && !hasValue2) {
        return 0;
      }
      if (hasValue1 && !hasValue2) {
        return 1;
      }
      if (!hasValue1 && hasValue2) {
        return -1;
      }

      // both side have value
      if (written1 != written2) {
        return written1 - written2;
      }
      int bodySize1 = WritableUtils.readVInt(buffer1);
      int bodySize2 = WritableUtils.readVInt(buffer2);
      Class<? extends Writable> clazz = Writables.WRITABLE_CODES.get(written1);
      if (WritableComparable.class.isAssignableFrom(clazz)) {
        int cmp = WritableComparator.get(clazz.asSubclass(WritableComparable.class)).compare(
            buffer1.getData(), buffer1.getPosition(), bodySize1,
            buffer2.getData(), buffer2.getPosition(), bodySize2);
        long skipped1 = buffer1.skip(bodySize1);
        long skipped2 = buffer2.skip(bodySize2);
        Preconditions.checkState(skipped1 == bodySize1);
        Preconditions.checkState(skipped2 == bodySize2);
        return cmp;
      } else {
        // fallback to deserialization
        Writable w1 = ReflectionUtils.newInstance(clazz, null);
        Writable w2 = ReflectionUtils.newInstance(clazz, null);
        w1.readFields(buffer1);
        w2.readFields(buffer2);
        return w1.hashCode() - w2.hashCode();
      }
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      return super.compare(a, b);
    }
  }

  static {
    // Register the comparator to Hadoop. It will be used to perform fast comparison over buffers
    // without any deserialization overhead.
    WritableComparator.define(TupleWritable.class, Comparator.getInstance());
  }
}
