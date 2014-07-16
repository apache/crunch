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

import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.TableSourceTarget;
import org.apache.crunch.io.avro.AvroFileSourceTarget;
import org.apache.crunch.io.seq.SeqFileSourceTarget;
import org.apache.crunch.io.seq.SeqFileTableSourceTarget;
import org.apache.crunch.io.text.TextFileSourceTarget;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

/**
 * <p>Static factory methods for creating common {@link SourceTarget} types, which may be treated as both a {@code Source}
 * and a {@code Target}.</p>
 * 
 * <p>The {@code At} methods is analogous to the {@link From} and {@link To} factory methods, but is used for
 * storing intermediate outputs that need to be passed from one run of a MapReduce pipeline to another run. The
 * {@code SourceTarget} object acts as both a {@code Source} and a {@Target}, which enables it to provide this
 * functionality.
 *
 * <pre>
 * {@code
 *
 *   Pipeline pipeline = new MRPipeline(this.getClass());
 *   // Create our intermediate storage location
 *   SourceTarget<String> intermediate = At.textFile("/temptext");
 *   ...
 *   // Write out the output of the first phase of a pipeline.
 *   pipeline.write(phase1, intermediate);
 *   
 *   // Explicitly call run to kick off the pipeline.
 *   pipeline.run();
 *   
 *   // And then kick off a second phase by consuming the output
 *   // from the first phase.
 *   PCollection<String> phase2Input = pipeline.read(intermediate);
 *   ...
 * }
 * </pre>
 * 
 * <p>The {@code SourceTarget} abstraction is useful when we care about reading the intermediate
 * outputs of a pipeline as well as the final results.</p>
 */
public class At {

  /**
   * Creates a {@code SourceTarget<T>} instance from the Avro file(s) at the given path name.
   * 
   * @param pathName The name of the path to the data on the filesystem
   * @param avroClass The subclass of {@code SpecificRecord} to use for the Avro file
   * @return A new {@code SourceTarget<T>} instance
   */
  public static <T extends SpecificRecord> SourceTarget<T> avroFile(String pathName, Class<T> avroClass) {
    return avroFile(new Path(pathName), avroClass);  
  }

  /**
   * Creates a {@code SourceTarget<T>} instance from the Avro file(s) at the given {@code Path}.
   * 
   * @param path The {@code Path} to the data
   * @param avroClass The subclass of {@code SpecificRecord} to use for the Avro file
   * @return A new {@code SourceTarget<T>} instance
   */
  public static <T extends SpecificRecord> SourceTarget<T> avroFile(Path path, Class<T> avroClass) {
    return avroFile(path, Avros.specifics(avroClass));  
  }

  /**
   * Creates a {@code SourceTarget<GenericData.Record>} by reading the schema of the Avro file
   * at the given path. If the path is a directory, the schema of a file in the directory
   * will be used to determine the schema to use.
   *
   * @param pathName The name of the path to the data on the filesystem
   * @return A new {@code SourceTarget<GenericData.Record>} instance
   */
  public static SourceTarget<GenericData.Record> avroFile(String pathName) {
    return avroFile(new Path(pathName));
  }

  /**
   * Creates a {@code SourceTarget<GenericData.Record>} by reading the schema of the Avro file
   * at the given path. If the path is a directory, the schema of a file in the directory
   * will be used to determine the schema to use.
   *
   * @param path The path to the data on the filesystem
   * @return A new {@code SourceTarget<GenericData.Record>} instance
   */
  public static SourceTarget<GenericData.Record> avroFile(Path path) {
    return avroFile(path, new Configuration());
  }

  /**
   * Creates a {@code SourceTarget<GenericData.Record>} by reading the schema of the Avro file
   * at the given path using the {@code FileSystem} information contained in the given
   * {@code Configuration} instance. If the path is a directory, the schema of a file in
   * the directory will be used to determine the schema to use.
   *
   * @param path The path to the data on the filesystem
   * @param conf The configuration information
   * @return A new {@code SourceTarget<GenericData.Record>} instance
   */
  public static SourceTarget<GenericData.Record> avroFile(Path path, Configuration conf) {
    return avroFile(path, Avros.generics(From.getSchemaFromPath(path, conf)));
  }

  /**
   * Creates a {@code SourceTarget<T>} instance from the Avro file(s) at the given path name.
   * 
   * @param pathName The name of the path to the data on the filesystem
   * @param ptype The {@code PType} for the Avro records
   * @return A new {@code SourceTarget<T>} instance
   */
  public static <T> SourceTarget<T> avroFile(String pathName, PType<T> ptype) {
    return avroFile(new Path(pathName), ptype);
  }

  /**
   * Creates a {@code SourceTarget<T>} instance from the Avro file(s) at the given {@code Path}.
   * 
   * @param path The {@code Path} to the data
   * @param ptype The {@code PType} for the Avro records
   * @return A new {@code SourceTarget<T>} instance
   */
  public static <T> SourceTarget<T> avroFile(Path path, PType<T> ptype) {
    return new AvroFileSourceTarget<T>(path, (AvroType<T>) ptype);
  }

  /**
   * Creates a {@code SourceTarget<T>} instance from the SequenceFile(s) at the given path name
   * from the value field of each key-value pair in the SequenceFile(s).
   * 
   * @param pathName The name of the path to the data on the filesystem
   * @param valueClass The {@code Writable} type for the value of the SequenceFile entry
   * @return A new {@code SourceTarget<T>} instance
   */
  public static <T extends Writable> SourceTarget<T> sequenceFile(String pathName, Class<T> valueClass) {
    return sequenceFile(new Path(pathName), valueClass);
  }

  /**
   * Creates a {@code SourceTarget<T>} instance from the SequenceFile(s) at the given {@code Path}
   * from the value field of each key-value pair in the SequenceFile(s).
   * 
   * @param path The {@code Path} to the data
   * @param valueClass The {@code Writable} type for the value of the SequenceFile entry
   * @return A new {@code SourceTarget<T>} instance
   */
  public static <T extends Writable> SourceTarget<T> sequenceFile(Path path, Class<T> valueClass) {
    return sequenceFile(path, Writables.writables(valueClass));
  }
  
  /**
   * Creates a {@code SourceTarget<T>} instance from the SequenceFile(s) at the given path name
   * from the value field of each key-value pair in the SequenceFile(s).
   * 
   * @param pathName The name of the path to the data on the filesystem
   * @param ptype The {@code PType} for the value of the SequenceFile entry
   * @return A new {@code SourceTarget<T>} instance
   */
  public static <T> SourceTarget<T> sequenceFile(String pathName, PType<T> ptype) {
    return sequenceFile(new Path(pathName), ptype);
  }

  /**
   * Creates a {@code SourceTarget<T>} instance from the SequenceFile(s) at the given {@code Path}
   * from the value field of each key-value pair in the SequenceFile(s).
   * 
   * @param path The {@code Path} to the data
   * @param ptype The {@code PType} for the value of the SequenceFile entry
   * @return A new {@code SourceTarget<T>} instance
   */
  public static <T> SourceTarget<T> sequenceFile(Path path, PType<T> ptype) {
    return new SeqFileSourceTarget<T>(path, ptype);
  }

  /**
   * Creates a {@code TableSourceTarget<K, V>} instance from the SequenceFile(s) at the given path name
   * from the key-value pairs in the SequenceFile(s).
   * 
   * @param pathName The name of the path to the data on the filesystem
   * @param keyClass The {@code Writable} type for the key of the SequenceFile entry
   * @param valueClass The {@code Writable} type for the value of the SequenceFile entry
   * @return A new {@code TableSourceTarget<K, V>} instance
   */
  public static <K extends Writable, V extends Writable> TableSourceTarget<K, V> sequenceFile(
      String pathName, Class<K> keyClass, Class<V> valueClass) {
    return sequenceFile(new Path(pathName), keyClass, valueClass);
  }

  /**
   * Creates a {@code TableSourceTarget<K, V>} instance from the SequenceFile(s) at the given {@code Path}
   * from the key-value pairs in the SequenceFile(s).
   * 
   * @param path The {@code Path} to the data
   * @param keyClass The {@code Writable} type for the key of the SequenceFile entry
   * @param valueClass The {@code Writable} type for the value of the SequenceFile entry
   * @return A new {@code TableSourceTarget<K, V>} instance
   */
  public static <K extends Writable, V extends Writable> TableSourceTarget<K, V> sequenceFile(
      Path path, Class<K> keyClass, Class<V> valueClass) {
    return sequenceFile(path, Writables.writables(keyClass), Writables.writables(valueClass));
  }
  
  /**
   * Creates a {@code TableSourceTarget<K, V>} instance from the SequenceFile(s) at the given path name
   * from the key-value pairs in the SequenceFile(s).
   * 
   * @param pathName The name of the path to the data on the filesystem
   * @param keyType The {@code PType} for the key of the SequenceFile entry
   * @param valueType The {@code PType} for the value of the SequenceFile entry
   * @return A new {@code TableSourceTarget<K, V>} instance
   */
  public static <K, V> TableSourceTarget<K, V> sequenceFile(String pathName, PType<K> keyType, PType<V> valueType) {
    return sequenceFile(new Path(pathName), keyType, valueType);
  }

  /**
   * Creates a {@code TableSourceTarget<K, V>} instance from the SequenceFile(s) at the given {@code Path}
   * from the key-value pairs in the SequenceFile(s).
   * 
   * @param path The {@code Path} to the data
   * @param keyType The {@code PType} for the key of the SequenceFile entry
   * @param valueType The {@code PType} for the value of the SequenceFile entry
   * @return A new {@code TableSourceTarget<K, V>} instance
   */
  public static <K, V> TableSourceTarget<K, V> sequenceFile(Path path, PType<K> keyType, PType<V> valueType) {
    PTypeFamily ptf = keyType.getFamily();
    return new SeqFileTableSourceTarget<K, V>(path, ptf.tableOf(keyType, valueType));
  }

  /**
   * Creates a {@code SourceTarget<String>} instance for the text file(s) at the given path name.
   * 
   * @param pathName The name of the path to the data on the filesystem
   * @return A new {@code SourceTarget<String>} instance
   */
  public static SourceTarget<String> textFile(String pathName) {
    return textFile(new Path(pathName));
  }

  /**
   * Creates a {@code SourceTarget<String>} instance for the text file(s) at the given {@code Path}.
   * 
   * @param path The {@code Path} to the data
   * @return A new {@code SourceTarget<String>} instance
   */
  public static SourceTarget<String> textFile(Path path) {
    return textFile(path, Writables.strings());
  }

  /**
   * Creates a {@code SourceTarget<T>} instance for the text file(s) at the given path name using
   * the provided {@code PType<T>} to convert the input text.
   * 
   * @param pathName The name of the path to the data on the filesystem
   * @param ptype The {@code PType<T>} to use to process the input text
   * @return A new {@code SourceTarget<T>} instance
   */
  public static <T> SourceTarget<T> textFile(String pathName, PType<T> ptype) {
    return textFile(new Path(pathName), ptype);
  }

  /**
   * Creates a {@code SourceTarget<T>} instance for the text file(s) at the given {@code Path} using
   * the provided {@code PType<T>} to convert the input text.
   * 
   * @param path The {@code Path} to the data
   * @param ptype The {@code PType<T>} to use to process the input text
   * @return A new {@code SourceTarget<T>} instance
   */
  public static <T> SourceTarget<T> textFile(Path path, PType<T> ptype) {
    return new TextFileSourceTarget<T>(path, ptype);
  }
}
