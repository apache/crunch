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

package com.cloudera.crunch.impl.mr.run;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.collect.Maps;

public class CrunchInputSplit extends InputSplit implements Configurable, Writable {

  private InputSplit inputSplit;
  private Class<? extends InputFormat> inputFormatClass;
  private Map<String, String> extraConf;
  private int nodeIndex;
  private Configuration conf;

  public CrunchInputSplit() {
    // default constructor
  }

  public CrunchInputSplit(InputSplit inputSplit,
      Class<? extends InputFormat> inputFormatClass, Map<String, String> extraConf,
      int nodeIndex, Configuration conf) {
    this.inputSplit = inputSplit;
    this.inputFormatClass = inputFormatClass;
    this.extraConf = extraConf;
    this.nodeIndex = nodeIndex;
    this.conf = conf;
  }

  public int getNodeIndex() {
    return nodeIndex;
  }

  public InputSplit getInputSplit() {
    return inputSplit;
  }

  public Class<? extends InputFormat> getInputFormatClass() {
    return inputFormatClass;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return inputSplit.getLength();
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return inputSplit.getLocations();
  }

  public void readFields(DataInput in) throws IOException {
    nodeIndex = in.readInt();
    int extraConfSize = in.readInt();
    if (extraConfSize > 0) {
      for (int i = 0; i < extraConfSize; i++) {
        conf.set(in.readUTF(), in.readUTF());
      }
    }
    inputFormatClass = (Class<? extends InputFormat>) readClass(in);
    Class<? extends InputSplit> inputSplitClass = (Class<? extends InputSplit>) readClass(in);
    inputSplit = (InputSplit) ReflectionUtils
        .newInstance(inputSplitClass, conf);
    SerializationFactory factory = new SerializationFactory(conf);
    Deserializer deserializer = factory.getDeserializer(inputSplitClass);
    deserializer.open((DataInputStream) in);
    inputSplit = (InputSplit) deserializer.deserialize(inputSplit);
  }

  private Class<?> readClass(DataInput in) throws IOException {
    String className = Text.readString(in);
    try {
      return conf.getClassByName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("readObject can't find class", e);
    }
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(nodeIndex);
    out.writeInt(extraConf.size());
    for (Map.Entry<String, String> e : extraConf.entrySet()) {
      out.writeUTF(e.getKey());
      out.writeUTF(e.getValue());
    }
    Text.writeString(out, inputFormatClass.getName());
    Text.writeString(out, inputSplit.getClass().getName());
    SerializationFactory factory = new SerializationFactory(conf);
    Serializer serializer = factory.getSerializer(inputSplit.getClass());
    serializer.open((DataOutputStream) out);
    serializer.serialize(inputSplit);
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }
}
