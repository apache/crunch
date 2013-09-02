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
package org.apache.crunch.io.hbase;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import org.apache.commons.codec.binary.Hex;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.io.impl.FileSourceImpl;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.List;

import static org.apache.crunch.types.writable.Writables.writables;

public class HFileSource extends FileSourceImpl<KeyValue> implements ReadableSource<KeyValue> {

  private static final PType<KeyValue> KEY_VALUE_PTYPE = writables(KeyValue.class);

  public HFileSource(Path path) {
    this(ImmutableList.of(path));
  }

  public HFileSource(List<Path> paths) {
    this(paths, new Scan());
  }

  // Package-local. Don't want it to be too open, because we only support limited filters yet
  // (namely start/stop row). Users who need advanced filters should use HFileUtils#scanHFiles.
  HFileSource(List<Path> paths, Scan scan) {
    super(paths, KEY_VALUE_PTYPE, createInputFormatBundle(scan));
  }

  private static FormatBundle<HFileInputFormat> createInputFormatBundle(Scan scan) {
    FormatBundle<HFileInputFormat> bundle = FormatBundle.forInput(HFileInputFormat.class);
    if (!Objects.equal(scan.getStartRow(), HConstants.EMPTY_START_ROW)) {
      bundle.set(HFileInputFormat.START_ROW_KEY, Hex.encodeHexString(scan.getStartRow()));
    }
    if (!Objects.equal(scan.getStopRow(), HConstants.EMPTY_END_ROW)) {
      bundle.set(HFileInputFormat.STOP_ROW_KEY, Hex.encodeHexString(scan.getStopRow()));
    }
    return bundle;
  }

  @Override
  public Iterable<KeyValue> read(Configuration conf) throws IOException {
    throw new UnsupportedOperationException("HFileSource#read(Configuration) is not implemented yet");
  }

  @Override
  public String toString() {
    return "HFile(" + pathsAsString() + ")";
  }
}
