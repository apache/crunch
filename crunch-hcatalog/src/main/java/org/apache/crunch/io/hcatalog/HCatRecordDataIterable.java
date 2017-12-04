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
package org.apache.crunch.io.hcatalog;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.io.FormatBundle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

public class HCatRecordDataIterable implements Iterable<HCatRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(HCatRecordDataIterable.class);

  private final FormatBundle<HCatInputFormat> bundle;
  private final Configuration conf;

  public HCatRecordDataIterable(FormatBundle<HCatInputFormat> bundle, Configuration configuration) {
    this.bundle = bundle;
    this.conf = configuration;
  }

  @Override
  public Iterator<HCatRecord> iterator() {
    try {
      Job job = Job.getInstance(bundle.configure(conf));

      final InputFormat fmt = ReflectionUtils.newInstance(bundle.getFormatClass(), conf);
      final TaskAttemptContext ctxt = new TaskAttemptContextImpl(conf, new TaskAttemptID());

      return Iterators.concat(Lists.transform(fmt.getSplits(job), new Function<InputSplit, Iterator<HCatRecord>>() {

        @Override
        public Iterator<HCatRecord> apply(InputSplit split) {
          RecordReader reader = null;
          try {
            reader = fmt.createRecordReader(split, ctxt);
            reader.initialize(split, ctxt);
          } catch (IOException | InterruptedException e) {
            throw new CrunchRuntimeException(e);
          }
          return new HCatRecordReaderIterator(reader);
        }
      }).iterator());
    } catch (Exception e) {
      throw new CrunchRuntimeException(e);
    }
  }

  private static class HCatRecordReaderIterator<T> implements Iterator<T> {

    private final RecordReader<WritableComparable, T> reader;
    private boolean hasNext;
    private T current;

    public HCatRecordReaderIterator(RecordReader reader) {
      this.reader = reader;
      try {
        hasNext = reader.nextKeyValue();
        if (hasNext)
          current = this.reader.getCurrentValue();
      } catch (IOException | InterruptedException e) {
        throw new CrunchRuntimeException(e);
      }
    }

    @Override
    public boolean hasNext() {
      return hasNext;
    }

    @Override
    public T next() {
      T ret = current;
      try {
        hasNext = reader.nextKeyValue();

        if (hasNext) {
          current = reader.getCurrentValue();
        }
      } catch (IOException | InterruptedException e) {
        throw new CrunchRuntimeException(e);
      }
      return ret;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Removing elements is not supported");
    }
  }
}
