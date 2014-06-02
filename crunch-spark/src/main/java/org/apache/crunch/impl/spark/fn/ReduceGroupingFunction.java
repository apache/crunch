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
package org.apache.crunch.impl.spark.fn;

import com.google.common.collect.Lists;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.impl.spark.ByteArray;
import org.apache.crunch.impl.spark.SparkRuntimeContext;
import org.apache.crunch.types.PGroupedTableType;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class ReduceGroupingFunction implements PairFlatMapFunction<Iterator<Tuple2<ByteArray, List<byte[]>>>,
    ByteArray, List<byte[]>> {

  private final GroupingOptions options;
  private final PGroupedTableType ptype;
  private final SparkRuntimeContext ctxt;
  private transient RawComparator<?> cmp;

  public ReduceGroupingFunction(GroupingOptions options,
                                PGroupedTableType ptype,
                                SparkRuntimeContext ctxt) {
    this.options = options;
    this.ptype = ptype;
    this.ctxt = ctxt;
  }

  @Override
  public Iterable<Tuple2<ByteArray, List<byte[]>>> call(
      final Iterator<Tuple2<ByteArray, List<byte[]>>> iter) throws Exception {
    return new Iterable<Tuple2<ByteArray, List<byte[]>>>() {
      @Override
      public Iterator<Tuple2<ByteArray, List<byte[]>>> iterator() {
        return new GroupingIterator(iter, rawComparator());
      }
    };
  }

  private RawComparator<?> rawComparator() {
    if (cmp == null) {
      try {
        Job job = new Job(ctxt.getConfiguration());
        ptype.configureShuffle(job, options);
        cmp = ReflectionUtils.newInstance(options.getGroupingComparatorClass(), job.getConfiguration());
      } catch (IOException e) {
        throw new CrunchRuntimeException("Error configuring grouping comparator", e);
      }
    }
    return cmp;
  }

  private static class GroupingIterator implements Iterator<Tuple2<ByteArray, List<byte[]>>> {

    private final Iterator<Tuple2<ByteArray, List<byte[]>>> iter;
    private final RawComparator cmp;
    private ByteArray key;
    private List<byte[]> bytes = Lists.newArrayList();

    public GroupingIterator(Iterator<Tuple2<ByteArray, List<byte[]>>> iter, RawComparator cmp) {
      this.iter = iter;
      this.cmp = cmp;
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext() || key != null;
    }

    @Override
    public Tuple2<ByteArray, List<byte[]>> next() {
      ByteArray nextKey = null;
      List<byte[]> next = null;
      while (iter.hasNext()) {
        Tuple2<ByteArray, List<byte[]>> t = iter.next();
        if (key == null) {
          key = t._1();
          bytes.addAll(t._2());
        } else if (cmp.compare(key.value, 0, key.value.length, t._1().value, 0, t._1().value.length) == 0) {
          bytes.addAll(t._2());
        } else {
          nextKey = t._1();
          next = Lists.newArrayList(t._2());
          break;
        }
      }
      Tuple2<ByteArray, List<byte[]>> ret = new Tuple2<ByteArray, List<byte[]>>(key, bytes);
      key = nextKey;
      bytes = next;
      return ret;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
