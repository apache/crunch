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
package org.apache.crunch.lib.sort;

import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryData;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.reflect.ReflectData;
import org.apache.crunch.lib.Sort.ColumnOrder;
import org.apache.crunch.lib.Sort.Order;
import org.apache.crunch.types.writable.TupleWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

/**
 * A collection of {@code RawComparator<T>} implementations that are used by Crunch's {@code Sort} library.
 */
public class Comparators {
  
  public static class ReverseWritableComparator<T> extends Configured implements RawComparator<T> {

    private RawComparator<T> comparator;

    @SuppressWarnings("unchecked")
    @Override
    public void setConf(Configuration conf) {
      super.setConf(conf);
      if (conf != null) {
        JobConf jobConf = new JobConf(conf);
        comparator = WritableComparator.get(jobConf.getMapOutputKeyClass().asSubclass(WritableComparable.class));
      }
    }

    @Override
    public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5) {
      return -comparator.compare(arg0, arg1, arg2, arg3, arg4, arg5);
    }

    @Override
    public int compare(T o1, T o2) {
      return -comparator.compare(o1, o2);
    }
  }

  public static class ReverseAvroComparator<T> extends Configured implements RawComparator<AvroKey<T>> {

    private Schema schema;

    @Override
    public void setConf(Configuration conf) {
      super.setConf(conf);
      if (conf != null) {
        schema = (new Schema.Parser()).parse(conf.get("crunch.schema"));
      }
    }

    @Override
    public int compare(AvroKey<T> o1, AvroKey<T> o2) {
      return -ReflectData.get().compare(o1.datum(), o2.datum(), schema);
    }

    @Override
    public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5) {
      return -BinaryData.compare(arg0, arg1, arg2, arg3, arg4, arg5, schema);
    }
  }

  public static class TupleWritableComparator extends WritableComparator implements Configurable {

    private static final String CRUNCH_ORDERING_PROPERTY = "crunch.ordering";

    private Configuration conf;
    private ColumnOrder[] columnOrders;

    public TupleWritableComparator() {
      super(TupleWritable.class, true);
    }

    public static void configureOrdering(Configuration conf, Order... orders) {
      conf.set(CRUNCH_ORDERING_PROPERTY,
          Joiner.on(",").join(Iterables.transform(Arrays.asList(orders), new Function<Order, String>() {
            @Override
            public String apply(Order o) {
              return o.name();
            }
          })));
    }

    public static void configureOrdering(Configuration conf, ColumnOrder... columnOrders) {
      conf.set(CRUNCH_ORDERING_PROPERTY,
          Joiner.on(",").join(Iterables.transform(Arrays.asList(columnOrders), new Function<ColumnOrder, String>() {
            @Override
            public String apply(ColumnOrder o) {
              return o.column() + ";" + o.order().name();
            }
          })));
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      TupleWritable ta = (TupleWritable) a;
      TupleWritable tb = (TupleWritable) b;
      for (int index = 0; index < columnOrders.length; index++) {
        int order = 1;
        if (columnOrders[index].order() == Order.ASCENDING) {
          order = 1;
        } else if (columnOrders[index].order() == Order.DESCENDING) {
          order = -1;
        } else { // ignore
          continue;
        }
        if (!ta.has(index) && !tb.has(index)) {
          continue;
        } else if (ta.has(index) && !tb.has(index)) {
          return order;
        } else if (!ta.has(index) && tb.has(index)) {
          return -order;
        } else {
          Writable v1 = ta.get(index);
          Writable v2 = tb.get(index);
          if (v1 != v2 && (v1 != null && !v1.equals(v2))) {
            if (v1 instanceof WritableComparable && v2 instanceof WritableComparable) {
              int cmp = ((WritableComparable) v1).compareTo((WritableComparable) v2);
              if (cmp != 0) {
                return order * cmp;
              }
            } else {
              int cmp = v1.hashCode() - v2.hashCode();
              if (cmp != 0) {
                return order * cmp;
              }
            }
          }
        }
      }
      return 0; // ordering using specified cols found no differences
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
      if (conf != null) {
        String ordering = conf.get(CRUNCH_ORDERING_PROPERTY);
        String[] columnOrderNames = ordering.split(",");
        columnOrders = new ColumnOrder[columnOrderNames.length];
        for (int i = 0; i < columnOrders.length; i++) {
          String[] split = columnOrderNames[i].split(";");
          int column = Integer.parseInt(split[0]);
          Order order = Order.valueOf(split[1]);
          columnOrders[i] = ColumnOrder.by(column, order);
        }
      }
    }
  }
}
