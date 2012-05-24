/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.crunch.io.impl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.cloudera.crunch.Pair;
import com.cloudera.crunch.TableSource;
import com.cloudera.crunch.types.PTableType;

public class FileTableSourceImpl<K, V> extends FileSourceImpl<Pair<K, V>>
	implements TableSource<K, V> {

  public FileTableSourceImpl(Path path, PTableType<K, V> tableType,
	  Class<? extends FileInputFormat> formatClass) {
	super(path, tableType, formatClass);
  }
  
  @Override
  public PTableType<K, V> getTableType() {
	return (PTableType<K, V>) getType();
  }
}
