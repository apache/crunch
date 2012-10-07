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

import org.apache.crunch.Target;
import org.apache.crunch.io.avro.AvroFileTarget;
import org.apache.crunch.io.impl.FileTargetImpl;
import org.apache.crunch.io.seq.SeqFileTarget;
import org.apache.crunch.io.text.TextFileTarget;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Static factory methods for creating various {@link Target} types.
 * 
 */
public class To {

  public static Target formattedFile(String pathName, Class<? extends FileOutputFormat> formatClass) {
    return formattedFile(new Path(pathName), formatClass);
  }

  public static Target formattedFile(Path path, Class<? extends FileOutputFormat> formatClass) {
    return new FileTargetImpl(path, formatClass, new SequentialFileNamingScheme());
  }

  public static Target avroFile(String pathName) {
    return avroFile(new Path(pathName));
  }

  public static Target avroFile(Path path) {
    return new AvroFileTarget(path);
  }

  public static Target sequenceFile(String pathName) {
    return sequenceFile(new Path(pathName));
  }

  public static Target sequenceFile(Path path) {
    return new SeqFileTarget(path);
  }

  public static Target textFile(String pathName) {
    return textFile(new Path(pathName));
  }

  public static Target textFile(Path path) {
    return new TextFileTarget(path);
  }

}
