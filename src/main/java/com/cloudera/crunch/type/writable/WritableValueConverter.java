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
package com.cloudera.crunch.type.writable;

import org.apache.hadoop.io.NullWritable;

import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.type.Converter;

class WritableValueConverter implements Converter<Object, Object, Object> {
  @Override
  public Object outputKey(Object input) {
    return NullWritable.get();
  }

  @Override
  public Object outputValue(Object input) {
    return input;
  }

  @Override
  public Object convertInput(Object key, Object value) {
    return value;
  }
}