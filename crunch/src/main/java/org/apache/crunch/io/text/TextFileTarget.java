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
package org.apache.crunch.io.text;

import org.apache.avro.Schema;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.io.FileNamingScheme;
import org.apache.crunch.io.SequentialFileNamingScheme;
import org.apache.crunch.io.impl.FileTargetImpl;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroTextOutputFormat;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.writable.WritableType;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TextFileTarget extends FileTargetImpl {
  private static Class<? extends FileOutputFormat> getOutputFormat(PType<?> ptype) {
    if (ptype.getFamily().equals(AvroTypeFamily.getInstance())) {
      return AvroTextOutputFormat.class;
    } else {
      return TextOutputFormat.class;
    }
  }

  public <T> TextFileTarget(String path) {
    this(new Path(path));
  }

  public <T> TextFileTarget(Path path) {
    this(path, new SequentialFileNamingScheme());
  }

  public <T> TextFileTarget(Path path, FileNamingScheme fileNamingScheme) {
    super(path, null, fileNamingScheme);
  }

  @Override
  public Path getPath() {
    return path;
  }

  @Override
  public String toString() {
    return "Text(" + path + ")";
  }

  @Override
  public void configureForMapReduce(Job job, PType<?> ptype, Path outputPath, String name) {
    Converter converter = ptype.getConverter();
    Class keyClass = converter.getKeyClass();
    Class valueClass = converter.getValueClass();
    configureForMapReduce(job, keyClass, valueClass, getOutputFormat(ptype), outputPath, name);
  }

  @Override
  public <T> SourceTarget<T> asSourceTarget(PType<T> ptype) {
    if (!isTextCompatible(ptype)) {
      return null;
    }
    if (ptype instanceof PTableType) {
      return new TextFileTableSourceTarget(path, (PTableType) ptype);
    }
    return new TextFileSourceTarget<T>(path, ptype);
  }
  
  private <T> boolean isTextCompatible(PType<T> ptype) {
    if (AvroTypeFamily.getInstance().equals(ptype.getFamily())) {
      AvroType<T> at = (AvroType<T>) ptype;
      if (at.getSchema().equals(Schema.create(Schema.Type.STRING))) {
        return true;
      }
    } else if (WritableTypeFamily.getInstance().equals(ptype.getFamily())) {
      if (ptype instanceof PTableType) {
        PTableType ptt = (PTableType) ptype;
        return isText(ptt.getKeyType()) && isText(ptt.getValueType());
      } else {
        return isText(ptype);
      }
    }
    return false;
  }
  
  private <T> boolean isText(PType<T> wtype) {
    return Text.class.equals(((WritableType) wtype).getSerializationClass());
  }
}
