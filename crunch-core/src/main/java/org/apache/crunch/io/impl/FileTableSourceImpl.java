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

import java.util.List;
import org.apache.crunch.Pair;
import org.apache.crunch.TableSource;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.types.PTableType;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class FileTableSourceImpl<K, V> extends FileSourceImpl<Pair<K, V>> implements TableSource<K, V> {

  public FileTableSourceImpl(Path path, PTableType<K, V> tableType, Class<? extends FileInputFormat> formatClass) {
    super(path, tableType, formatClass);
  }

  public FileTableSourceImpl(List<Path> paths, PTableType<K, V> tableType, Class<? extends FileInputFormat> formatClass) {
    super(paths, tableType, formatClass);
  }

  public FileTableSourceImpl(Path path, PTableType<K, V> tableType, FormatBundle bundle) {
    super(path, tableType, bundle);
  }

  public FileTableSourceImpl(List<Path> paths, PTableType<K, V> tableType, FormatBundle bundle) {
    super(paths, tableType, bundle);
  }
  
  @Override
  public PTableType<K, V> getTableType() {
    return (PTableType<K, V>) getType();
  }
}
