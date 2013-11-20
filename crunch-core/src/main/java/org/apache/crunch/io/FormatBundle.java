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
package org.apache.crunch.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;

import com.google.common.collect.Maps;

/**
 * A combination of an {@link InputFormat} or {@link OutputFormat} and any extra 
 * configuration information that format class needs to run.
 * 
 * <p>The {@code FormatBundle} allow us to let different formats act as
 * if they are the only format that exists in a particular MapReduce job, even
 * when we have multiple types of inputs and outputs within a single job.
 */
public class FormatBundle<K> implements Serializable, Writable, Configurable {

  private Class<K> formatClass;
  private Map<String, String> extraConf;
  private Configuration conf;
  
  public static <T> FormatBundle<T> fromSerialized(String serialized, Configuration conf) {
    ByteArrayInputStream bais = new ByteArrayInputStream(Base64.decodeBase64(serialized));
    try {
      FormatBundle<T> bundle = new FormatBundle<T>();
      bundle.setConf(conf);
      bundle.readFields(new DataInputStream(bais));
      return bundle;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T extends InputFormat<?, ?>> FormatBundle<T> forInput(Class<T> inputFormatClass) {
    return new FormatBundle<T>(inputFormatClass);
  }
  
  public static <T extends OutputFormat<?, ?>> FormatBundle<T> forOutput(Class<T> outputFormatClass) {
    return new FormatBundle<T>(outputFormatClass);
  }
  
  public FormatBundle() {
    // For Writable support
  }
  
  private FormatBundle(Class<K> formatClass) {
    this.formatClass = formatClass;
    this.extraConf = Maps.newHashMap();
  }

  public FormatBundle<K> set(String key, String value) {
    this.extraConf.put(key, value);
    return this;
  }

  public Class<K> getFormatClass() {
    return formatClass;
  }

  public Configuration configure(Configuration conf) {
    for (Map.Entry<String, String> e : extraConf.entrySet()) {
      conf.set(e.getKey(), e.getValue());
    }
    return conf;
  }

  public String serialize() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      DataOutputStream dos = new DataOutputStream(baos);
      write(dos);
      return Base64.encodeBase64String(baos.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public String getName() {
    return formatClass.getSimpleName();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(formatClass).append(extraConf).toHashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof FormatBundle)) {
      return false;
    }
    FormatBundle<K> oib = (FormatBundle<K>) other;
    return formatClass.equals(oib.formatClass) && extraConf.equals(oib.extraConf);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.formatClass = readClass(in);
    int ecSize = in.readInt();
    this.extraConf = Maps.newHashMap();
    for (int i = 0; i  < ecSize; i++) {
      String key = Text.readString(in);
      String value = Text.readString(in);
      extraConf.put(key, value);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, formatClass.getName());
    out.writeInt(extraConf.size());
    for (Map.Entry<String, String> e : extraConf.entrySet()) {
      Text.writeString(out, e.getKey());
      Text.writeString(out, e.getValue());
    }
  }
  
  private Class readClass(DataInput in) throws IOException {
    String className = Text.readString(in);
    try {
      return conf.getClassByName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("readObject can't find class", e);
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
}
