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

import java.io.IOException;

import org.apache.crunch.io.FileNamingScheme;
import org.apache.crunch.io.PathTarget;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.io.ReadableSourceTarget;
import org.apache.hadoop.conf.Configuration;

public class ReadableSourcePathTargetImpl<T> extends SourcePathTargetImpl<T> implements ReadableSourceTarget<T> {

  public ReadableSourcePathTargetImpl(ReadableSource<T> source, PathTarget target, FileNamingScheme fileNamingScheme) {
    super(source, target, fileNamingScheme);
  }

  @Override
  public Iterable<T> read(Configuration conf) throws IOException {
    return ((ReadableSource<T>) source).read(conf);
  }

}
