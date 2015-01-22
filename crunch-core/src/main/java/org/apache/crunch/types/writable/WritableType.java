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

import java.io.IOException;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.crunch.MapFn;
import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.io.ReadableSourceTarget;
import org.apache.crunch.io.seq.SeqFileSource;
import org.apache.crunch.io.seq.SeqFileSourceTarget;
import org.apache.crunch.io.text.NLineFileSource;
import org.apache.crunch.io.text.TextFileSource;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.DeepCopier;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WritableType<T, W extends Writable> implements PType<T> {

  private static final Logger LOG = LoggerFactory.getLogger(WritableType.class);

  private final Class<T> typeClass;
  private final Class<W> writableClass;
  private final Converter converter;
  private final MapFn<W, T> inputFn;
  private final MapFn<T, W> outputFn;
  private final DeepCopier<W> deepCopier;
  private final List<PType> subTypes;
  private boolean initialized = false;

  /**
   * Factory method for a new WritableType instance whose type class is immutable.
   * <p/>
   * No checking is done to ensure that instances of the type class are immutable, but deep copying will be skipped
   * for instances denoted by the created PType.
   */
  public static <T, W extends Writable> WritableType<T, W> immutableType(Class<T> typeClass, Class<W> writableClass,
                                                                         MapFn<W, T> inputDoFn, MapFn<T, W> outputDoFn,
                                                                         PType... subTypes) {
    return new WritableType<T, W>(typeClass, writableClass, inputDoFn, outputDoFn,
                                  null, subTypes);
  }

  public WritableType(Class<T> typeClass, Class<W> writableClass, MapFn<W, T> inputDoFn,
                       MapFn<T, W> outputDoFn, PType... subTypes) {
    this(typeClass, writableClass, inputDoFn, outputDoFn, new WritableDeepCopier<W>(writableClass), subTypes);
  }

  private WritableType(Class<T> typeClass, Class<W> writableClass, MapFn<W, T> inputDoFn,
      MapFn<T, W> outputDoFn, DeepCopier<W> deepCopier, PType... subTypes) {
    this.typeClass = typeClass;
    this.writableClass = writableClass;
    this.inputFn = inputDoFn;
    this.outputFn = outputDoFn;
    this.converter = new WritableValueConverter(writableClass);
    this.deepCopier = deepCopier;
    this.subTypes = ImmutableList.<PType> builder().add(subTypes).build();
  }

  @Override
  public PTypeFamily getFamily() {
    return WritableTypeFamily.getInstance();
  }

  @Override
  public Class<T> getTypeClass() {
    return typeClass;
  }

  @Override
  public Converter getConverter() {
    return converter;
  }

  @Override
  public MapFn getInputMapFn() {
    return inputFn;
  }

  @Override
  public MapFn getOutputMapFn() {
    return outputFn;
  }

  @Override
  public List<PType> getSubTypes() {
    return subTypes;
  }

  public Class<W> getSerializationClass() {
    return writableClass;
  }

  @Override
  public ReadableSourceTarget<T> getDefaultFileSource(Path path) {
    return new SeqFileSourceTarget<T>(path, this);
  }

  @Override
  public ReadableSource<T> createSourceTarget(Configuration conf, Path path, Iterable<T> contents, int parallelism)
    throws IOException {
    FileSystem fs = FileSystem.get(conf);
    outputFn.setConfiguration(conf);
    outputFn.initialize();
    if (Text.class.equals(writableClass) && parallelism > 1) {
      FSDataOutputStream out = fs.create(path);
      byte[] newLine = "\r\n".getBytes(Charsets.UTF_8);
      double contentSize = 0;
      for (T value : contents) {
        Text txt = (Text) outputFn.map(value);
        out.write(txt.toString().getBytes(Charsets.UTF_8));
        out.write(newLine);
        contentSize++;
      }
      out.close();
      return new NLineFileSource<T>(path, this, (int) Math.ceil(contentSize / parallelism));
    } else { // Use sequence files
      fs.mkdirs(path);
      List<SequenceFile.Writer> writers = Lists.newArrayListWithExpectedSize(parallelism);
      for (int i = 0; i < parallelism; i++) {
        Path out = new Path(path, "out" + i);
        writers.add(SequenceFile.createWriter(fs, conf, out, NullWritable.class, writableClass));
      }
      int target = 0;
      for (T value : contents) {
        writers.get(target).append(NullWritable.get(), outputFn.map(value));
        target = (target + 1) % parallelism;
      }
      for (SequenceFile.Writer writer : writers) {
        writer.close();
      }
      ReadableSource<T> ret = new SeqFileSource<T>(path, this);
      ret.inputConf(RuntimeParameters.DISABLE_COMBINE_FILE, "true");
      return ret;
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof WritableType)) {
      return false;
    }
    WritableType wt = (WritableType) obj;
    return (typeClass.equals(wt.typeClass) && writableClass.equals(wt.writableClass) && subTypes
        .equals(wt.subTypes));
  }

  @Override
  public void initialize(Configuration conf) {
    this.inputFn.setConfiguration(conf);
    this.outputFn.setConfiguration(conf);
    this.inputFn.initialize();
    this.outputFn.initialize();
    for (PType subType : subTypes) {
      subType.initialize(conf);
    }
    this.initialized = true;
  }

  @Override
  public T getDetachedValue(T value) {
    if (deepCopier == null) {
      return value;
    }
    if (!initialized) {
      throw new IllegalStateException("Cannot call getDetachedValue on an uninitialized PType");
    }
    W writableValue = outputFn.map(value);
    W deepCopy = this.deepCopier.deepCopy(writableValue);
    return inputFn.map(deepCopy);
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder();
    hcb.append(typeClass).append(writableClass).append(subTypes);
    return hcb.toHashCode();
  }
}
