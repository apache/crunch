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
package org.apache.crunch.impl.mr.run;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;

import com.google.common.base.Supplier;
import org.apache.crunch.io.FormatBundle;
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

class CrunchInputSplit extends InputSplit implements Writable, Configurable, Supplier<InputSplit> {

  private InputSplit inputSplit;
  private int nodeIndex;
  private FormatBundle<? extends InputFormat<?, ?>> bundle;
  private Configuration conf;

  public CrunchInputSplit() {
    // default constructor
  }

  public CrunchInputSplit(
      InputSplit inputSplit,
      FormatBundle<? extends InputFormat<?, ?>> bundle,
      int nodeIndex,
      Configuration conf) {
    this.inputSplit = inputSplit;
    this.bundle = bundle;
    this.nodeIndex = nodeIndex;
    this.conf = conf;
  }

  @Override
  public InputSplit get() {
    return inputSplit;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = new Configuration(conf);
    if (bundle != null) {
      this.bundle.configure(this.conf);
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  public int getNodeIndex() {
    return nodeIndex;
  }

  public Class<? extends InputFormat<?, ?>> getInputFormatClass() {
    return bundle.getFormatClass();
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return inputSplit.getLength();
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return inputSplit.getLocations();
  }

  @Override
  public String toString() {
    return String.format("CrunchInputSplit(%s)", inputSplit);
  }

  public void readFields(DataInput in) throws IOException {
    if (conf == null) {
      conf = new Configuration();
    }
    nodeIndex = in.readInt();
    bundle = new FormatBundle();
    bundle.setConf(conf);
    bundle.readFields(in);
    bundle.configure(conf); // yay bootstrap!
    Class<? extends InputSplit> inputSplitClass = readClass(in);
    inputSplit = (InputSplit) ReflectionUtils.newInstance(inputSplitClass, conf);
    if (inputSplit instanceof Writable) {
      ((Writable) inputSplit).readFields(in);
    } else {
      SerializationFactory factory = new SerializationFactory(conf);
      Deserializer deserializer = factory.getDeserializer(inputSplitClass);
      deserializer.open((DataInputStream) in);
      inputSplit = (InputSplit) deserializer.deserialize(inputSplit);
      deserializer.close();
    }
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(nodeIndex);
    bundle.write(out);
    Text.writeString(out, inputSplit.getClass().getName());
    if (inputSplit instanceof Writable) {
      ((Writable) inputSplit).write(out);
    } else {
      SerializationFactory factory = new SerializationFactory(conf);
      Serializer serializer = factory.getSerializer(inputSplit.getClass());
      serializer.open((OutputStream) out);
      serializer.serialize(inputSplit);
      serializer.close();
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
}
