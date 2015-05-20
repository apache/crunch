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
package org.apache.crunch.io.impl;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.io.FileReaderFactory;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

class DefaultFileReaderFactory<T> implements FileReaderFactory<T> {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultFileReaderFactory.class);

  private final FormatBundle<? extends InputFormat> bundle;
  private final PType<T> ptype;

  public DefaultFileReaderFactory(FormatBundle<? extends InputFormat> bundle, PType<T> ptype) {
    this.bundle = bundle;
    this.ptype = ptype;
  }

  @Override
  public Iterator<T> read(FileSystem fs, Path path) {
    final Configuration conf = new Configuration(fs.getConf());
    bundle.configure(conf);
    ptype.initialize(conf);

    final InputFormat fmt = ReflectionUtils.newInstance(bundle.getFormatClass(), conf);
    final TaskAttemptContext ctxt = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    try {
      Job job = new Job(conf);
      FileInputFormat.addInputPath(job, path);
      return Iterators.concat(Lists.transform(fmt.getSplits(job), new Function<InputSplit, Iterator<T>>() {
        @Override
        public Iterator<T> apply(InputSplit split) {
          try {
            RecordReader reader = fmt.createRecordReader(split, ctxt);
            reader.initialize(split, ctxt);
            return new RecordReaderIterator<T>(reader, ptype);
          } catch (Exception e) {
            LOG.error("Error reading split: " + split, e);
            throw new CrunchRuntimeException(e);
          }
        }
      }).iterator());
    } catch (Exception e) {
      LOG.error("Error reading path: " + path, e);
      throw new CrunchRuntimeException(e);
    }
  }

  private static class RecordReaderIterator<T> extends UnmodifiableIterator<T> {

    private final RecordReader reader;
    private final PType<T> ptype;
    private T cur;
    private boolean hasNext;

    public RecordReaderIterator(RecordReader reader, PType<T> ptype) {
      this.reader = reader;
      this.ptype = ptype;
      try {
        this.hasNext = reader.nextKeyValue();
        if (hasNext) {
          Object converted = ptype.getConverter().convertInput(
                  reader.getCurrentKey(), reader.getCurrentValue());
          this.cur = ptype.getInputMapFn().map(converted);
        }
      } catch (Exception e) {
        throw new CrunchRuntimeException(e);
      }
    }

    @Override
    public boolean hasNext() {
      return hasNext;
    }

    @Override
    public T next() {
      T ret = cur;
      try {
        hasNext = reader.nextKeyValue();
        if (hasNext) {
          Object converted = ptype.getConverter().convertInput(
                  reader.getCurrentKey(), reader.getCurrentValue());
          this.cur = ptype.getInputMapFn().map(converted);
        }
      } catch (Exception e) {
        throw new CrunchRuntimeException(e);
      }
      return ret;
    }
  }
}
