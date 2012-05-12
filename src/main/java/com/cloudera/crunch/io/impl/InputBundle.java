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
 * InputFormats pretend as if they are the only InputFormat that exists in
 * a particular MapReduce job.
 */
public class InputBundle implements Serializable {

  private Class<? extends InputFormat> inputFormatClass;
  private Map<String, String> extraConf;
  
  public static InputBundle fromSerialized(String serialized) {
    ByteArrayInputStream bais = new ByteArrayInputStream(Base64.decodeBase64(serialized));
    try {
      ObjectInputStream ois = new ObjectInputStream(bais);
      InputBundle bundle = (InputBundle) ois.readObject();
      ois.close();
      return bundle;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
  
  public InputBundle(Class<? extends InputFormat> inputFormatClass) {
    this.inputFormatClass = inputFormatClass;
    this.extraConf = Maps.newHashMap();
  }
  
  public InputBundle set(String key, String value) {
    this.extraConf.put(key, value);
    return this;
  }
  
  public Class<? extends InputFormat> getInputFormatClass() {
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
    InputBundle oib = (InputBundle) other;
    return inputFormatClass.equals(oib.inputFormatClass) && extraConf.equals(oib.extraConf);
  }
}
