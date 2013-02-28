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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.lib.Sort.ColumnOrder;
import org.apache.crunch.lib.Sort.Order;
import org.apache.crunch.types.writable.TupleWritable;
import org.apache.crunch.types.writable.WritableType;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import org.apache.hadoop.io.WritableFactories;

public class TupleWritableComparator extends WritableComparator implements Configurable {

  private static final String CRUNCH_ORDERING_PROPERTY = "crunch.ordering";

  private Configuration conf;
  Writable[] w1;
  Writable[] w2;
  private ColumnOrder[] columnOrders;

  public TupleWritableComparator() {
    super(TupleWritable.class, true);
  }

  public static void configureOrdering(Configuration conf, WritableType[] types, ColumnOrder[] columnOrders) {
    List<String> ordering = Lists.newArrayList();
    for (int i = 0; i < types.length; i++) {
      Class<?> cls = types[i].getSerializationClass();
      String order = columnOrders[i].order().name();
      ordering.add(cls.getCanonicalName() + ";" + order);
    }
    conf.set(CRUNCH_ORDERING_PROPERTY, Joiner.on(",").join(ordering));
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
        BytesWritable v1 = ta.get(index);
        BytesWritable v2 = tb.get(index);
        if (v1 != v2 && (v1 != null && !v1.equals(v2))) {
          try {
            w1[index].readFields(new DataInputStream(new ByteArrayInputStream(v1.getBytes())));
            w2[index].readFields(new DataInputStream(new ByteArrayInputStream(v2.getBytes())));
          } catch (IOException e) {
            throw new CrunchRuntimeException(e);
          }
          if (w1[index] instanceof WritableComparable && w2[index] instanceof WritableComparable) {
            int cmp = ((WritableComparable) w1[index]).compareTo((WritableComparable) w2[index]);
            if (cmp != 0) {
              return order * cmp;
            }
          } else {
            int cmp = w1[index].hashCode() - w2[index].hashCode();
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
      w1 = new Writable[columnOrderNames.length];
      w2 = new Writable[columnOrderNames.length];
      for (int i = 0; i < columnOrders.length; i++) {
        String[] split = columnOrderNames[i].split(";");
        String className = split[0];
        try {
          Class cls = Class.forName(className);
          w1[i] = WritableFactories.newInstance(cls);
          w2[i] = WritableFactories.newInstance(cls);
        } catch (Exception e) {
          throw new CrunchRuntimeException(e);
        }
        Order order = Order.valueOf(split[1]);
        columnOrders[i] = ColumnOrder.by(i + 1, order);
      }
    }
  }
}

