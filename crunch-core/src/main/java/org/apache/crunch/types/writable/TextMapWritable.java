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
package org.apache.crunch.types.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.collect.Maps;

class TextMapWritable implements Writable {

  private final Map<Text, BytesWritable> instance;

  public TextMapWritable() {
    this.instance = Maps.newHashMap();
  }

  public void put(Text txt, BytesWritable value) {
    instance.put(txt, value);
  }

  public Set<Map.Entry<Text, BytesWritable>> entrySet() {
    return instance.entrySet();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    instance.clear();
    int entries = WritableUtils.readVInt(in);
    for (int i = 0; i < entries; i++) {
      Text txt = new Text();
      txt.readFields(in);
      BytesWritable value = new BytesWritable();
      value.readFields(in);
      instance.put(txt, value);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, instance.size());
    for (Map.Entry<Text, BytesWritable> e : instance.entrySet()) {
      e.getKey().write(out);
      e.getValue().write(out);
    }
  }

}
