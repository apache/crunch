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

import org.apache.crunch.Pair;
import org.apache.crunch.TableSourceTarget;
import org.apache.crunch.io.FileNamingScheme;
import org.apache.crunch.io.SequentialFileNamingScheme;
import org.apache.crunch.io.impl.ReadableSourcePathTargetImpl;
import org.apache.crunch.types.PTableType;
import org.apache.hadoop.fs.Path;

/**
 * A {@code TableSource} and {@code SourceTarget} implementation that uses the
 * {@code KeyValueTextInputFormat} and {@code TextOutputFormat} to support reading
 * and writing text files as {@code PTable} instances using a tab separator for
 * the keys and the values.
 */
public class TextFileTableSourceTarget<K, V> extends ReadableSourcePathTargetImpl<Pair<K, V>> implements
    TableSourceTarget<K, V> {

  private final PTableType<K, V> tableType;
  
  public TextFileTableSourceTarget(String path, PTableType<K, V> tableType) {
    this(new Path(path), tableType);
  }

  public TextFileTableSourceTarget(Path path, PTableType<K, V> tableType) {
    this(path, tableType, SequentialFileNamingScheme.getInstance());
  }

  public TextFileTableSourceTarget(Path path, PTableType<K, V> tableType,
      FileNamingScheme fileNamingScheme) {
    super(new TextFileTableSource<K, V>(path, tableType), new TextFileTarget(path),
        fileNamingScheme);
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
