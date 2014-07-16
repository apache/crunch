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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * <p>Static factory methods for creating common {@link Target} types.</p>
 * 
 * <p>The {@code To} class is intended to be used as part of a literate API
 * for writing the output of Crunch pipelines to common file types. We can use
 * the {@code Target} objects created by the factory methods in the {@code To}
 * class with either the {@code write} method on the {@code Pipeline} class or
 * the convenience {@code write} method on {@code PCollection} and {@code PTable}
 * instances.
 * 
 * <pre>
 * {@code
 *
 *   Pipeline pipeline = new MRPipeline(this.getClass());
 *   ...
 *   // Write a PCollection<String> to a text file:
 *   PCollection<String> words = ...;
 *   pipeline.write(words, To.textFile("/put/my/words/here"));
 *   
 *   // Write a PTable<Text, Text> to a sequence file:
 *   PTable<Text, Text> textToText = ...;
 *   textToText.write(To.sequenceFile("/words/to/words"));
 *   
 *   // Write a PCollection<MyAvroObject> to an Avro data file:
 *   PCollection<MyAvroObject> objects = ...;
 *   objects.write(To.avroFile("/my/avro/files"));
 *   
 *   // Write a PTable to a custom FileOutputFormat:
 *   PTable<KeyWritable, ValueWritable> custom = ...;
 *   pipeline.write(custom, To.formattedFile("/custom", MyFileFormat.class));
 * }
 * </pre>
 */
public class To {

  /**
   * Creates a {@code Target} at the given path name that writes data to
   * a custom {@code FileOutputFormat}.
   * 
   * @param pathName The name of the path to write the data to on the filesystem
   * @param formatClass The {@code FileOutputFormat<K, V>} to write the data to
   * @return A new {@code Target} instance
   */
  public static <K extends Writable, V extends Writable> Target formattedFile(
      String pathName, Class<? extends FileOutputFormat<K, V>> formatClass) {
    return formattedFile(new Path(pathName), formatClass);
  }

  /**
   * Creates a {@code Target} at the given {@code Path} that writes data to
   * a custom {@code FileOutputFormat}.
   * 
   * @param path The {@code Path} to write the data to
   * @param formatClass The {@code FileOutputFormat} to write the data to
   * @return A new {@code Target} instance
   */
  public static <K extends Writable, V extends Writable> Target formattedFile(
      Path path, Class<? extends FileOutputFormat<K, V>> formatClass) {
    return new FileTargetImpl(path, formatClass, SequentialFileNamingScheme.getInstance());
  }

  /**
   * Creates a {@code Target} at the given path name that writes data to
   * Avro files. The {@code PType} for the written data must be for Avro records.
   * 
   * @param pathName The name of the path to write the data to on the filesystem
   * @return A new {@code Target} instance
   */
  public static Target avroFile(String pathName) {
    return avroFile(new Path(pathName));
  }

  /**
   * Creates a {@code Target} at the given {@code Path} that writes data to
   * Avro files. The {@code PType} for the written data must be for Avro records.
   * 
   * @param path The {@code Path} to write the data to
   * @return A new {@code Target} instance
   */
  public static Target avroFile(Path path) {
    return new AvroFileTarget(path);
  }

  /**
   * Creates a {@code Target} at the given path name that writes data to
   * SequenceFiles.
   * 
   * @param pathName The name of the path to write the data to on the filesystem
   * @return A new {@code Target} instance
   */
  public static Target sequenceFile(String pathName) {
    return sequenceFile(new Path(pathName));
  }

  /**
   * Creates a {@code Target} at the given {@code Path} that writes data to
   * SequenceFiles.
   * 
   * @param path The {@code Path} to write the data to
   * @return A new {@code Target} instance
   */
  public static Target sequenceFile(Path path) {
    return new SeqFileTarget(path);
  }

  /**
   * Creates a {@code Target} at the given path name that writes data to
   * text files.
   * 
   * @param pathName The name of the path to write the data to on the filesystem
   * @return A new {@code Target} instance
   */
  public static Target textFile(String pathName) {
    return textFile(new Path(pathName));
  }

  /**
   * Creates a {@code Target} at the given {@code Path} that writes data to
   * text files.
   * 
   * @param path The {@code Path} to write the data to
   * @return A new {@code Target} instance
   */
  public static Target textFile(Path path) {
    return new TextFileTarget(path);
  }
}
