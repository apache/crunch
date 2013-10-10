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
package org.apache.crunch.io.text;

import java.io.IOException;

import java.util.List;

import org.apache.crunch.ReadableData;
import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.io.impl.FileSourceImpl;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;

/**
 * A {@code Source} instance that uses the {@code NLineInputFormat}, which gives each map
 * task a fraction of the lines in a text file as input. Most useful when running simulations
 * on Hadoop, where each line represents configuration information about each simulation
 * run.
 */
public class NLineFileSource<T> extends FileSourceImpl<T> implements ReadableSource<T> {

  private static FormatBundle getBundle(int linesPerTask) {
    FormatBundle bundle = FormatBundle.forInput(NLineInputFormat.class);
    bundle.set(NLineInputFormat.LINES_PER_MAP, String.valueOf(linesPerTask));
    bundle.set(RuntimeParameters.DISABLE_COMBINE_FILE, "true");
    return bundle;
  }
  
  /**
   * Create a new {@code NLineFileSource} instance.
   * 
   * @param path The path to the input data, as a String
   * @param ptype The PType to use for processing the data
   * @param linesPerTask The number of lines from the input each map task will process
   */
  public NLineFileSource(String path, PType<T> ptype, int linesPerTask) {
    this(new Path(path), ptype, linesPerTask);
  }
  
  /**
   * Create a new {@code NLineFileSource} instance.
   *  
   * @param path The {@code Path} to the input data
   * @param ptype The PType to use for processing the data
   * @param linesPerTask The number of lines from the input each map task will process
   */
  public NLineFileSource(Path path, PType<T> ptype, int linesPerTask) {
    super(path, ptype, getBundle(linesPerTask));
  }

  /**
   * Create a new {@code NLineFileSource} instance.
   *
   * @param paths The {@code Path}s to the input data
   * @param ptype The PType to use for processing the data
   * @param linesPerTask The number of lines from the input each map task will process
   */
  public NLineFileSource(List<Path> paths, PType<T> ptype, int linesPerTask) {
    super(paths, ptype, getBundle(linesPerTask));
  }

  @Override
  public String toString() {
    return "NLine(" + pathsAsString() + ")";
  }
  
  @Override
  public Iterable<T> read(Configuration conf) throws IOException {
    return read(conf, new TextFileReaderFactory<T>(LineParser.forType(ptype)));
  }

  @Override
  public ReadableData<T> asReadable() {
    return new TextReadableData<T>(paths, ptype);
  }
}
