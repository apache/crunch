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

import java.io.IOException;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.crunch.ReadableData;
import org.apache.crunch.Source;
import org.apache.crunch.impl.mr.run.CrunchInputFormat;
import org.apache.crunch.io.CompositePathIterable;
import org.apache.crunch.io.CrunchInputs;
import org.apache.crunch.io.FileReaderFactory;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.io.SourceTargetHelper;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSourceImpl<T> implements ReadableSource<T> {

  private static final Logger LOG = LoggerFactory.getLogger(FileSourceImpl.class);

  @Deprecated
  protected final Path path;
  protected final List<Path> paths;
  protected final PType<T> ptype;
  protected final FormatBundle<? extends InputFormat> inputBundle;

  public FileSourceImpl(Path path, PType<T> ptype, Class<? extends InputFormat> inputFormatClass) {
    this(path, ptype, FormatBundle.forInput(inputFormatClass));
  }

  public FileSourceImpl(Path path, PType<T> ptype, FormatBundle<? extends InputFormat> inputBundle) {
    this(Lists.newArrayList(path), ptype, inputBundle);
  }

  public FileSourceImpl(List<Path> paths, PType<T> ptype, Class<? extends InputFormat> inputFormatClass) {
    this(paths, ptype, FormatBundle.forInput(inputFormatClass));
  }

  public FileSourceImpl(List<Path> paths, PType<T> ptype, FormatBundle<? extends InputFormat> inputBundle) {
    Preconditions.checkArgument(!paths.isEmpty(), "Must supply at least one input path");
    this.path = paths.get(0);
    this.paths = paths;
    this.ptype = ptype;
    this.inputBundle = inputBundle;
  }

  @Deprecated
  public Path getPath() {
    if (paths.size() > 1) {
      LOG.warn("getPath() called for source with multiple paths, only returning first. Source: {}", this);
    }
    return paths.get(0);
  }

  public List<Path> getPaths() {
    return paths;
  }

  @Override
  public Source<T> inputConf(String key, String value) {
    inputBundle.set(key, value);
    return this;
  }

  @Override
  public Converter<?, ?, ?, ?> getConverter() {
    return ptype.getConverter();
  }
  
  @Override
  public void configureSource(Job job, int inputId) throws IOException {
    // Use Crunch to handle the combined input splits
    job.setInputFormatClass(CrunchInputFormat.class);
    CrunchInputs.addInputPaths(job, paths, inputBundle, inputId);
  }

  public FormatBundle<? extends InputFormat> getBundle() {
    return inputBundle;
  }

  @Override
  public PType<T> getType() {
    return ptype;
  }

  @Override
  public long getSize(Configuration configuration) {
    long size = 0;
    for (Path path : paths) {
      try {
        size += SourceTargetHelper.getPathSize(configuration, path);
      } catch (IOException e) {
        LOG.warn("Exception thrown looking up size of: {}", path, e);
        throw new IllegalStateException("Failed to get the file size of:" + path, e);
      }
    }
    return size;
  }

  protected Iterable<T> read(Configuration conf, FileReaderFactory<T> readerFactory)
      throws IOException {
    List<Iterable<T>> iterables = Lists.newArrayList();
    for (Path path : paths) {
      FileSystem fs = path.getFileSystem(conf);
      iterables.add(CompositePathIterable.create(fs, path, readerFactory));
    }
    return Iterables.concat(iterables);
  }

  /* Retain string format for single-path sources */
  protected String pathsAsString() {
    if (paths.size() == 1) {
      return paths.get(0).toString();
    }
    return paths.toString();
  }

  @Override
  public long getLastModifiedAt(Configuration conf) {
    long lastMod = -1;
    for (Path path : paths) {
      try {
        FileSystem fs = path.getFileSystem(conf);
        long lm = SourceTargetHelper.getLastModifiedAt(fs, path);
        if (lm > lastMod) {
          lastMod = lm;
        }
      } catch (IOException e) {
        LOG.error("Could not determine last modification time for source: {}", toString(), e);
      }
    }
    return lastMod;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !getClass().equals(other.getClass())) {
      return false;
    }
    FileSourceImpl o = (FileSourceImpl) other;
    return ptype.equals(o.ptype) && paths.equals(o.paths) && inputBundle.equals(o.inputBundle);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(ptype).append(paths).append(inputBundle).toHashCode();
  }

  @Override
  public String toString() {
    return new StringBuilder().append(inputBundle.getName()).append("(").append(pathsAsString()).append(")").toString();
  }

  @Override
  public Iterable<T> read(Configuration conf) throws IOException {
    return read(conf, new DefaultFileReaderFactory<T>(inputBundle, ptype));
  }

  @Override
  public ReadableData<T> asReadable() {
    return new FileReadableData<T>(paths, inputBundle, ptype);
  }
}
