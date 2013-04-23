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

import org.apache.avro.specific.SpecificRecord;
import org.apache.crunch.Source;
import org.apache.crunch.TableSource;
import org.apache.crunch.io.avro.AvroFileSource;
import org.apache.crunch.io.impl.FileTableSourceImpl;
import org.apache.crunch.io.seq.SeqFileSource;
import org.apache.crunch.io.seq.SeqFileTableSource;
import org.apache.crunch.io.text.TextFileSource;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * <p>Static factory methods for creating common {@link Source} types.</p>
 * 
 * <p>The {@code From} class is intended to provide a literate API for creating
 * Crunch pipelines from common input file types.
 * 
 * <code>
 *   Pipeline pipeline = new MRPipeline(this.getClass());
 *   
 *   // Reference the lines of a text file by wrapping the TextInputFormat class.
 *   PCollection<String> lines = pipeline.read(From.textFile("/path/to/myfiles"));
 *   
 *   // Reference entries from a sequence file where the key is a LongWritable and the
 *   // value is a custom Writable class.
 *   PTable<LongWritable, MyWritable> table = pipeline.read(From.sequenceFile(
 *       "/path/to/seqfiles", LongWritable.class, MyWritable.class));
 *   
 *   // Reference the records from an Avro file, where MyAvroObject implements Avro's
 *   // SpecificRecord interface.
 *   PCollection<MyAvroObject> myObjects = pipeline.read(From.avroFile("/path/to/avrofiles",
 *       MyAvroObject.class));
 *       
 *   // References the key-value pairs from a custom extension of FileInputFormat:
 *   PTable<KeyWritable, ValueWritable> custom = pipeline.read(From.formattedFile(
 *       "/custom", MyFileInputFormat.class, KeyWritable.class, ValueWritable.class));
 * </code>
 * </p>
 */
public class From {

  /**
   * Creates a {@code TableSource<K, V>} for reading data from files that have custom
   * {@code FileInputFormat<K, V>} implementations not covered by the provided {@code TableSource}
   * and {@code Source} factory methods.
   * 
   * @param pathName The name of the path to the data on the filesystem
   * @param formatClass The {@code FileInputFormat} implementation
   * @param keyClass The {@code Writable} to use for the key
   * @param valueClass The {@code Writable} to use for the value
   * @return A new {@code TableSource<K, V>} instance
   */
  public static <K extends Writable, V extends Writable> TableSource<K, V> formattedFile(
      String pathName, Class<? extends FileInputFormat<K, V>> formatClass,
      Class<K> keyClass, Class<V> valueClass) {
    return formattedFile(new Path(pathName), formatClass, keyClass, valueClass);
  }

  /**
   * Creates a {@code TableSource<K, V>} for reading data from files that have custom
   * {@code FileInputFormat<K, V>} implementations not covered by the provided {@code TableSource}
   * and {@code Source} factory methods.
   * 
   * @param  The {@code Path} to the data
   * @param formatClass The {@code FileInputFormat} implementation
   * @param keyClass The {@code Writable} to use for the key
   * @param valueClass The {@code Writable} to use for the value
   * @return A new {@code TableSource<K, V>} instance
   */
  public static <K extends Writable, V extends Writable> TableSource<K, V> formattedFile(
      Path path, Class<? extends FileInputFormat<K, V>> formatClass,
      Class<K> keyClass, Class<V> valueClass) {
    return formattedFile(path, formatClass, Writables.writables(keyClass),
        Writables.writables(valueClass));
  }

  /**
   * Creates a {@code TableSource<K, V>} for reading data from files that have custom
   * {@code FileInputFormat} implementations not covered by the provided {@code TableSource}
   * and {@code Source} factory methods.
   * 
   * @param pathName The name of the path to the data on the filesystem
   * @param formatClass The {@code FileInputFormat} implementation
   * @param keyType The {@code PType} to use for the key
   * @param valueType The {@code PType} to use for the value
   * @return A new {@code TableSource<K, V>} instance
   */
  public static <K, V> TableSource<K, V> formattedFile(String pathName,
      Class<? extends FileInputFormat<?, ?>> formatClass,
      PType<K> keyType, PType<V> valueType) {
    return formattedFile(new Path(pathName), formatClass, keyType, valueType);
  }

  /**
   * Creates a {@code TableSource<K, V>} for reading data from files that have custom
   * {@code FileInputFormat} implementations not covered by the provided {@code TableSource}
   * and {@code Source} factory methods.
   * 
   * @param  The {@code Path} to the data
   * @param formatClass The {@code FileInputFormat} implementation
   * @param keyType The {@code PType} to use for the key
   * @param valueType The {@code PType} to use for the value
   * @return A new {@code TableSource<K, V>} instance
   */
  public static <K, V> TableSource<K, V> formattedFile(Path path,
      Class<? extends FileInputFormat<?, ?>> formatClass,
      PType<K> keyType, PType<V> valueType) {
    PTableType<K, V> tableType = keyType.getFamily().tableOf(keyType, valueType);
    return new FileTableSourceImpl<K, V>(path, tableType, formatClass);
  }

  /**
   * Creates a {@code Source<T>} instance from the Avro file(s) at the given path name.
   * 
   * @param pathName The name of the path to the data on the filesystem
   * @param avroClass The subclass of {@code SpecificRecord} to use for the Avro file
   * @return A new {@code Source<T>} instance
   */
  public static <T extends SpecificRecord> Source<T> avroFile(String pathName, Class<T> avroClass) {
    return avroFile(new Path(pathName), avroClass);  
  }

  /**
   * Creates a {@code Source<T>} instance from the Avro file(s) at the given {@code Path}.
   * 
   * @param path The {@code Path} to the data
   * @param avroClass The subclass of {@code SpecificRecord} to use for the Avro file
   * @return A new {@code Source<T>} instance
   */
  public static <T extends SpecificRecord> Source<T> avroFile(Path path, Class<T> avroClass) {
    return avroFile(path, Avros.specifics(avroClass));  
  }
  
  /**
   * Creates a {@code Source<T>} instance from the Avro file(s) at the given path name.
   * 
   * @param pathName The name of the path to the data on the filesystem
   * @param avroType The {@code AvroType} for the Avro records
   * @return A new {@code Source<T>} instance
   */
  public static <T> Source<T> avroFile(String pathName, AvroType<T> avroType) {
    return avroFile(new Path(pathName), avroType);
  }

  /**
   * Creates a {@code Source<T>} instance from the Avro file(s) at the given {@code Path}.
   * 
   * @param path The {@code Path} to the data
   * @param avroType The {@code AvroType} for the Avro records
   * @return A new {@code Source<T>} instance
   */
  public static <T> Source<T> avroFile(Path path, AvroType<T> avroType) {
    return new AvroFileSource<T>(path, avroType);
  }

  /**
   * Creates a {@code Source<T>} instance from the SequenceFile(s) at the given path name
   * from the value field of each key-value pair in the SequenceFile(s).
   * 
   * @param pathName The name of the path to the data on the filesystem
   * @param valueClass The {@code Writable} type for the value of the SequenceFile entry
   * @return A new {@code Source<T>} instance
   */
  public static <T extends Writable> Source<T> sequenceFile(String pathName, Class<T> valueClass) {
    return sequenceFile(new Path(pathName), valueClass);
  }
  
  /**
   * Creates a {@code Source<T>} instance from the SequenceFile(s) at the given {@code Path}
   * from the value field of each key-value pair in the SequenceFile(s).
   * 
   * @param path The {@code Path} to the data
   * @param valueClass The {@code Writable} type for the value of the SequenceFile entry
   * @return A new {@code Source<T>} instance
   */
  public static <T extends Writable> Source<T> sequenceFile(Path path, Class<T> valueClass) {
    return sequenceFile(path, Writables.writables(valueClass));
  }
  
  /**
   * Creates a {@code Source<T>} instance from the SequenceFile(s) at the given path name
   * from the value field of each key-value pair in the SequenceFile(s).
   * 
   * @param pathName The name of the path to the data on the filesystem
   * @param ptype The {@code PType} for the value of the SequenceFile entry
   * @return A new {@code Source<T>} instance
   */
  public static <T> Source<T> sequenceFile(String pathName, PType<T> ptype) {
    return sequenceFile(new Path(pathName), ptype);
  }

  /**
   * Creates a {@code Source<T>} instance from the SequenceFile(s) at the given {@code Path}
   * from the value field of each key-value pair in the SequenceFile(s).
   * 
   * @param path The {@code Path} to the data
   * @param ptype The {@code PType} for the value of the SequenceFile entry
   * @return A new {@code Source<T>} instance
   */
  public static <T> Source<T> sequenceFile(Path path, PType<T> ptype) {
    return new SeqFileSource<T>(path, ptype);
  }

  /**
   * Creates a {@code TableSource<K, V>} instance for the SequenceFile(s) at the given path name.
   * 
   * @param pathName The name of the path to the data on the filesystem
   * @param keyClass The {@code Writable} subclass for the key of the SequenceFile entry
   * @param valueClass The {@code Writable} subclass for the value of the SequenceFile entry
   * @return A new {@code SourceTable<K, V>} instance
   */
  public static <K extends Writable, V extends Writable> TableSource<K, V> sequenceFile(
      String pathName, Class<K> keyClass, Class<V> valueClass) {
    return sequenceFile(new Path(pathName), keyClass, valueClass);
  }

  /**
   * Creates a {@code TableSource<K, V>} instance for the SequenceFile(s) at the given {@code Path}.
   * 
   * @param path The {@code Path} to the data
   * @param keyClass The {@code Writable} subclass for the key of the SequenceFile entry
   * @param valueClass The {@code Writable} subclass for the value of the SequenceFile entry
   * @return A new {@code SourceTable<K, V>} instance
   */
  public static <K extends Writable, V extends Writable> TableSource<K, V> sequenceFile(
      Path path, Class<K> keyClass, Class<V> valueClass) {
    return sequenceFile(path, Writables.writables(keyClass), Writables.writables(valueClass));
  }
  
  /**
   * Creates a {@code TableSource<K, V>} instance for the SequenceFile(s) at the given path name.
   * 
   * @param pathName The name of the path to the data on the filesystem
   * @param keyType The {@code PType} for the key of the SequenceFile entry
   * @param valueType The {@code PType} for the value of the SequenceFile entry
   * @return A new {@code SourceTable<K, V>} instance
   */
  public static <K, V> TableSource<K, V> sequenceFile(String pathName, PType<K> keyType, PType<V> valueType) {
    return sequenceFile(new Path(pathName), keyType, valueType);
  }

  /**
   * Creates a {@code TableSource<K, V>} instance for the SequenceFile(s) at the given {@code Path}.
   * 
   * @param path The {@code Path} to the data
   * @param keyType The {@code PType} for the key of the SequenceFile entry
   * @param valueType The {@code PType} for the value of the SequenceFile entry
   * @return A new {@code SourceTable<K, V>} instance
   */
  public static <K, V> TableSource<K, V> sequenceFile(Path path, PType<K> keyType, PType<V> valueType) {
    PTypeFamily ptf = keyType.getFamily();
    return new SeqFileTableSource<K, V>(path, ptf.tableOf(keyType, valueType));
  }

  /**
   * Creates a {@code Source<String>} instance for the text file(s) at the given path name.
   * 
   * @param pathName The name of the path to the data on the filesystem
   * @return A new {@code Source<String>} instance
   */
  public static Source<String> textFile(String pathName) {
    return textFile(new Path(pathName));
  }

  /**
   * Creates a {@code Source<String>} instance for the text file(s) at the given {@code Path}.
   * 
   * @param path The {@code Path} to the data
   * @return A new {@code Source<String>} instance
   */
  public static Source<String> textFile(Path path) {
    return textFile(path, Writables.strings());
  }

  /**
   * Creates a {@code Source<T>} instance for the text file(s) at the given path name using
   * the provided {@code PType<T>} to convert the input text.
   * 
   * @param pathName The name of the path to the data on the filesystem
   * @param ptype The {@code PType<T>} to use to process the input text
   * @return A new {@code Source<T>} instance
   */
  public static <T> Source<T> textFile(String pathName, PType<T> ptype) {
    return textFile(new Path(pathName), ptype);
  }

  /**
   * Creates a {@code Source<T>} instance for the text file(s) at the given {@code Path} using
   * the provided {@code PType<T>} to convert the input text.
   * 
   * @param path The {@code Path} to the data
   * @param ptype The {@code PType<T>} to use to process the input text
   * @return A new {@code Source<T>} instance
   */
  public static <T> Source<T> textFile(Path path, PType<T> ptype) {
    return new TextFileSource<T>(path, ptype);
  }
}
