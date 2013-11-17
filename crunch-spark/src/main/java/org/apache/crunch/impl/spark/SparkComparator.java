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

import org.apache.avro.mapred.AvroKeyComparator;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.types.PGroupedTableType;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.writable.WritableType;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;

public class SparkComparator implements Comparator<ByteArray>, Serializable {

  private final Class<? extends RawComparator> cmpClass;
  private final GroupingOptions options;
  private final PGroupedTableType ptype;
  private final SparkRuntimeContext ctxt;
  private transient RawComparator<?> cmp;

  public SparkComparator(GroupingOptions options,
                         PGroupedTableType ptype,
                         SparkRuntimeContext ctxt) {
    if (options.getSortComparatorClass() != null) {
      this.cmpClass = options.getSortComparatorClass();
    } else if (AvroTypeFamily.getInstance().equals(ptype.getFamily())) {
      this.cmpClass = AvroKeyComparator.class;
    } else {
      this.cmpClass = null;
    }
    this.options = options;
    this.ptype = ptype;
    this.ctxt = ctxt;
  }

  @Override
  public int compare(ByteArray s1, ByteArray s2) {
    byte[] b1 = s1.value;
    byte[] b2 = s2.value;
    return rawComparator().compare(b1, 0, b1.length, b2, 0, b2.length);
  }

  private RawComparator<?> rawComparator() {
    if (cmp == null) {
      try {
        ptype.initialize(ctxt.getConfiguration());
        Job job = new Job(ctxt.getConfiguration());
        ptype.configureShuffle(job, options);
        if (cmpClass != null) {
          cmp = ReflectionUtils.newInstance(cmpClass, job.getConfiguration());
        } else {
          cmp = WritableComparator.get(((WritableType) ptype.getTableType().getKeyType()).getSerializationClass());
          if (cmp == null) {
            cmp = new ByteWritable.Comparator();
          }
        }
      } catch (IOException e) {
        throw new CrunchRuntimeException("Error configuring sort comparator", e);
      }
    }
    return cmp;
  }
}
