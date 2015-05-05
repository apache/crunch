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

import org.apache.crunch.io.FileReaderFactory;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.types.PType;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;

import java.util.List;

class FileReadableData<T> extends ReadableDataImpl<T> {

  private final FormatBundle<? extends InputFormat> bundle;
  private final PType<T> ptype;

  public FileReadableData(List<Path> paths, FormatBundle<? extends InputFormat> bundle, PType<T> ptype) {
    super(paths);
    this.bundle = bundle;
    this.ptype = ptype;
  }
  @Override
  protected FileReaderFactory<T> getFileReaderFactory() {
    return new DefaultFileReaderFactory<T>(bundle, ptype);
  }
}
