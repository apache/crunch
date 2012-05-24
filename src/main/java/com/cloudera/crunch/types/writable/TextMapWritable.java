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

package com.cloudera.crunch.types.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.collect.Maps;

public class TextMapWritable<T extends Writable> implements Writable {

  private Class<T> valueClazz;
  private final Map<Text, T> instance;
  
  public TextMapWritable() {
	this.instance = Maps.newHashMap();
  }
  
  public TextMapWritable(Class<T> valueClazz) {
	this.valueClazz = valueClazz;
	this.instance = Maps.newHashMap();
  }
  
  public void put(Text txt, T value) {
	instance.put(txt, value);
  }
  
  public Set<Map.Entry<Text, T>> entrySet() {
	return instance.entrySet();
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
	instance.clear();
	try {
	  this.valueClazz = (Class<T>) Class.forName(Text.readString(in));
	} catch (ClassNotFoundException e) {
	  throw (IOException) new IOException("Failed map init").initCause(e);
	}
	int entries = WritableUtils.readVInt(in);
	try {
	  for (int i = 0; i < entries; i++) {
	    Text txt = new Text();
	    txt.readFields(in);
		T value = valueClazz.newInstance();
  	    value.readFields(in);
	    instance.put(txt, value);
	  }
	} catch (IllegalAccessException e) {
	  throw (IOException) new IOException("Failed map init").initCause(e);
	} catch (InstantiationException e) {
      throw (IOException) new IOException("Failed map init").initCause(e);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
	Text.writeString(out, valueClazz.getName());
	WritableUtils.writeVInt(out, instance.size());
	for (Map.Entry<Text, T> e : instance.entrySet()) {
	  e.getKey().write(out);
	  e.getValue().write(out);
	}
  }

}
