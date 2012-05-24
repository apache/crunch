/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
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
package com.cloudera.crunch.io.seq;

import org.apache.hadoop.fs.Path;

import com.cloudera.crunch.Pair;
import com.cloudera.crunch.TableSource;
import com.cloudera.crunch.io.impl.ReadableSourcePathTargetImpl;
import com.cloudera.crunch.types.PTableType;

public class SeqFileTableSourceTarget<K, V> extends ReadableSourcePathTargetImpl<Pair<K,V>> implements TableSource<K, V> {
  private final PTableType<K, V> tableType;
  
  public SeqFileTableSourceTarget(String path, PTableType<K, V> tableType) {
    this(new Path(path), tableType);
  }
  
  public SeqFileTableSourceTarget(Path path, PTableType<K, V> tableType) {
    super(new SeqFileTableSource<K, V>(path, tableType), new SeqFileTarget(path));
    this.tableType = tableType;
  }
  
  @Override
  public PTableType<K, V> getTableType() {
    return tableType;
  }
  
  @Override
  public String toString() {
    return target.toString();
  }
}
