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
package org.apache.crunch.io.orc;

import java.io.IOException;
import java.util.List;

import org.apache.crunch.ReadableData;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.io.impl.FileSourceImpl;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;

public class OrcFileSource<T> extends FileSourceImpl<T> implements ReadableSource<T> {
  
  private int[] readColumns;
    
  public static final String HIVE_READ_ALL_COLUMNS = "hive.io.file.read.all.columns";
  
  private static <S> FormatBundle<OrcCrunchInputFormat> getBundle(int[] readColumns) {
    FormatBundle<OrcCrunchInputFormat> fb = FormatBundle.forInput(OrcCrunchInputFormat.class);
    if (readColumns != null) {  // setting configurations for column pruning
      fb.set(HIVE_READ_ALL_COLUMNS, "false");
      fb.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, getColumnIdsStr(readColumns));
    }
    return fb;
  }
  
  static String getColumnIdsStr(int[] columns) {
    StringBuilder sb = new StringBuilder();
    for (int c : columns) {
      sb.append(c);
      sb.append(',');
    }
    return sb.length() > 0 ? sb.substring(0, sb.length() - 1) : "";
  }
  
  public OrcFileSource(Path path, PType<T> ptype) {
    this(path, ptype, null);
  }
  
  /**
   * Constructor for column pruning optimization
   * 
   * @param path
   * @param ptype
   * @param readColumns columns which will be read
   */
  public OrcFileSource(Path path, PType<T> ptype, int[] readColumns) {
    super(path, ptype, getBundle(readColumns));
    this.readColumns = readColumns;
  }
  
  public OrcFileSource(List<Path> paths, PType<T> ptype) {
    this(paths, ptype, null);
  }
  
  /**
   * Constructor for column pruning optimization
   * 
   * @param paths
   * @param ptype
   * @param columns columns which will be reserved
   */
  public OrcFileSource(List<Path> paths, PType<T> ptype, int[] columns) {
    super(paths, ptype, getBundle(columns));
  }
  
  @Override
  public String toString() {
    return "Orc(" + pathsAsString() + ")";
  }

  @Override
  public Iterable<T> read(Configuration conf) throws IOException {
    return read(conf, new OrcFileReaderFactory<T>(ptype, readColumns));
  }

  @Override
  public ReadableData<T> asReadable() {
    return new OrcReadableData<T>(this.paths, ptype, readColumns);
  }

}
