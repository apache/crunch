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
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;

import com.google.common.collect.Maps;

/**
 * A combination of an InputFormat and any configuration information that
 * InputFormat needs to run properly. InputBundles allow us to let different
 * InputFormats act as if they are the only InputFormat that exists in a
 * particular MapReduce job.
 */
public class InputBundle<K extends InputFormat> implements Serializable {

  private Class<K> inputFormatClass;
  private Map<String, String> extraConf;

  public static <T extends InputFormat> InputBundle<T> fromSerialized(String serialized) {
    ByteArrayInputStream bais = new ByteArrayInputStream(Base64.decodeBase64(serialized));
    try {
      ObjectInputStream ois = new ObjectInputStream(bais);
      InputBundle<T> bundle = (InputBundle<T>) ois.readObject();
      ois.close();
      return bundle;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T extends InputFormat> InputBundle<T> of(Class<T> inputFormatClass) {
    return new InputBundle<T>(inputFormatClass);
  }
  
  public InputBundle(Class<K> inputFormatClass) {
    this.inputFormatClass = inputFormatClass;
    this.extraConf = Maps.newHashMap();
  }

  public InputBundle<K> set(String key, String value) {
    this.extraConf.put(key, value);
    return this;
  }

  public Class<K> getInputFormatClass() {
    return inputFormatClass;
  }

  public Map<String, String> getExtraConfiguration() {
    return extraConf;
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
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(this);
      oos.close();
      return Base64.encodeBase64String(baos.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public String getName() {
    return inputFormatClass.getSimpleName();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(inputFormatClass).append(extraConf).toHashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof InputBundle)) {
      return false;
    }
    InputBundle<K> oib = (InputBundle<K>) other;
    return inputFormatClass.equals(oib.inputFormatClass) && extraConf.equals(oib.extraConf);
  }
}
