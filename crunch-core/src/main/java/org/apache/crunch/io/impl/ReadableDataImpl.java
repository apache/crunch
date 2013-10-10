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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.DoFn;
import org.apache.crunch.ReadableData;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.io.CompositePathIterable;
import org.apache.crunch.io.FileReaderFactory;
import org.apache.crunch.util.DistCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public abstract class ReadableDataImpl<T> implements ReadableData<T> {

  private List<String> paths;
  private transient SourceTarget parent;

  protected ReadableDataImpl(List<Path> paths) {
    this.paths = Lists.newArrayList();
    for (Path p : paths) {
      this.paths.add(p.toString());
    }
  }

  public ReadableData<T> setParent(SourceTarget<?> parent) {
    this.parent = parent;
    return this;
  }

  @Override
  public Set<SourceTarget<?>> getSourceTargets() {
    if (parent != null) {
      return ImmutableSet.<SourceTarget<?>>of(parent);
    } else {
      return ImmutableSet.of();
    }
  }


  @Override
  public void configure(Configuration conf) {
    for (String path : paths) {
      DistCache.addCacheFile(new Path(path), conf);
    }
  }

  protected abstract FileReaderFactory<T> getFileReaderFactory();

  private Path getCacheFilePath(String input, Configuration conf) {
    Path local = DistCache.getPathToCacheFile(new Path(input), conf);
    if (local == null) {
      throw new CrunchRuntimeException("Can't find local cache file for '" + input + "'");
    }
    return local;
  }

  @Override
  public Iterable<T> read(TaskInputOutputContext<?, ?, ?, ?> ctxt) throws IOException {
    final Configuration conf = ctxt.getConfiguration();
    final FileReaderFactory<T> readerFactory = getFileReaderFactory();
    return Iterables.concat(Lists.transform(paths, new Function<String, Iterable<T>>() {
      @Override
      public Iterable<T> apply(@Nullable String input) {
        Path path = getCacheFilePath(input, conf);
        try {
          FileSystem fs = path.getFileSystem(conf);
          return CompositePathIterable.create(fs, path, readerFactory);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }));
  }
}
