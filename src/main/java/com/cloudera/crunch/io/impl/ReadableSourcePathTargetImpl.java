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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.crunch.io.PathTarget;
import com.cloudera.crunch.io.ReadableSource;
import com.cloudera.crunch.io.ReadableSourceTarget;

public class ReadableSourcePathTargetImpl<T> extends SourcePathTargetImpl<T>
	implements ReadableSourceTarget<T> {

  public ReadableSourcePathTargetImpl(ReadableSource<T> source, PathTarget target) {
	super(source, target);
  }
  
  @Override
  public Iterable<T> read(Configuration conf) throws IOException {
	return ((ReadableSource<T>) source).read(conf);
  }

}
