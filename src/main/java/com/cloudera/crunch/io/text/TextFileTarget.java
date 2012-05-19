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
package com.cloudera.crunch.io.text;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.cloudera.crunch.SourceTarget;
import com.cloudera.crunch.io.impl.FileTargetImpl;
import com.cloudera.crunch.types.PTableType;
import com.cloudera.crunch.types.PType;

public class TextFileTarget extends FileTargetImpl {
  public TextFileTarget(String path) {
    this(new Path(path));
  }
  
  public TextFileTarget(Path path) {
    super(path, TextOutputFormat.class);
  }
  
  @Override
  public Path getPath() {
    return path;
  }

  @Override
  public String toString() {
    return "Text(" + path + ")";
  }

  @Override
  public <T> SourceTarget<T> asSourceTarget(PType<T> ptype) {
    if (ptype instanceof PTableType) {
      return null;
    }
    return new TextFileSourceTarget<T>(path, ptype);
  }
}
