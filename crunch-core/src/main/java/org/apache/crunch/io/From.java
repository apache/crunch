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

import java.io.IOException;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificRecord;
import org.apache.crunch.Pair;
import org.apache.crunch.Source;
import org.apache.crunch.TableSource;
import org.apache.crunch.io.avro.AvroFileSource;
import org.apache.crunch.io.avro.AvroTableFileSource;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * <p>Static factory methods for creating common {@link Source} types.</p>
 * 
 * <p>The {@code From} class is intended to provide a literate API for creating
 * Crunch pipelines from common input file types.
 *
 * <pre>
 * {@code
 *
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
 * }
 * </pre>
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
   * @param path The {@code Path} to the data
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
   * {@code FileInputFormat<K, V>} implementations not covered by the provided {@code TableSource}
   * and {@code Source} factory methods.
   *
   * @param paths A list of {@code Path}s to the data
   * @param formatClass The {@code FileInputFormat} implementation
   * @param keyClass The {@code Writable} to use for the key
   * @param valueClass The {@code Writable} to use for the value
   * @return A new {@code TableSource<K, V>} instance
   */
  public static <K extends Writable, V extends Writable> TableSource<K, V> formattedFile(
      List<Path> paths, Class<? extends FileInputFormat<K, V>> formatClass,
      Class<K> keyClass, Class<V> valueClass) {
    return formattedFile(paths, formatClass, Writables.writables(keyClass),
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
   * @param path The {@code Path} to the data
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
   * Creates a {@code TableSource<K, V>} for reading data from files that have custom
   * {@code FileInputFormat} implementations not covered by the provided {@code TableSource}
   * and {@code Source} factory methods.
   *
   * @param paths A list of {@code Path}s to the data
   * @param formatClass The {@code FileInputFormat} implementation
   * @param keyType The {@code PType} to use for the key
   * @param valueType The {@code PType} to use for the value
   * @return A new {@code TableSource<K, V>} instance
   */
  public static <K, V> TableSource<K, V> formattedFile(List<Path> paths,
                                                       Class<? extends FileInputFormat<?, ?>> formatClass,
                                                       PType<K> keyType, PType<V> valueType) {
    PTableType<K, V> tableType = keyType.getFamily().tableOf(keyType, valueType);
    return new FileTableSourceImpl<K, V>(paths, tableType, formatClass);
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
   * Creates a {@code Source<T>} instance from the Avro file(s) at the given {@code Path}s.
   *
   * @param paths A list of {@code Path}s to the data
   * @param avroClass The subclass of {@code SpecificRecord} to use for the Avro file
   * @return A new {@code Source<T>} instance
   */
  public static <T extends SpecificRecord> Source<T> avroFile(List<Path> paths, Class<T> avroClass) {
    return avroFile(paths, Avros.specifics(avroClass));
  }

  /**
   * Creates a {@code Source<T>} instance from the Avro file(s) at the given path name.
   * 
   * @param pathName The name of the path to the data on the filesystem
   * @param ptype The {@code AvroType} for the Avro records
   * @return A new {@code Source<T>} instance
   */
  public static <T> Source<T> avroFile(String pathName, PType<T> ptype) {
    return avroFile(new Path(pathName), ptype);
  }

  /**
   * Creates a {@code Source<T>} instance from the Avro file(s) at the given {@code Path}.
   * 
   * @param path The {@code Path} to the data
   * @param ptype The {@code AvroType} for the Avro records
   * @return A new {@code Source<T>} instance
   */
  public static <T> Source<T> avroFile(Path path, PType<T> ptype) {
    return new AvroFileSource<T>(path, (AvroType<T>) ptype);
  }

  /**
   * Creates a {@code Source<T>} instance from the Avro file(s) at the given {@code Path}s.
   *
   * @param paths A list of {@code Path}s to the data
   * @param ptype The {@code PType} for the Avro records
   * @return A new {@code Source<T>} instance
   */
  public static <T> Source<T> avroFile(List<Path> paths, PType<T> ptype) {
    return new AvroFileSource<T>(paths, (AvroType<T>) ptype);
  }

  /**
   * Creates a {@code Source<GenericData.Record>} by reading the schema of the Avro file
   * at the given path. If the path is a directory, the schema of a file in the directory
   * will be used to determine the schema to use.
   *
   * @param pathName The name of the path to the data on the filesystem
   * @return A new {@code Source<GenericData.Record>} instance
   */
  public static Source<GenericData.Record> avroFile(String pathName) {
    return avroFile(new Path(pathName));
  }

  /**
   * Creates a {@code Source<GenericData.Record>} by reading the schema of the Avro file
   * at the given path. If the path is a directory, the schema of a file in the directory
   * will be used to determine the schema to use.
   *
   * @param path The path to the data on the filesystem
   * @return A new {@code Source<GenericData.Record>} instance
   */
  public static Source<GenericData.Record> avroFile(Path path) {
    return avroFile(path, new Configuration());
  }

  /**
   * Creates a {@code Source<GenericData.Record>} by reading the schema of the Avro file
   * at the given paths. If the path is a directory, the schema of a file in the directory
   * will be used to determine the schema to use.
   *
   * @param paths A list of paths to the data on the filesystem
   * @return A new {@code Source<GenericData.Record>} instance
   */
  public static Source<GenericData.Record> avroFile(List<Path> paths) {
    return avroFile(paths, new Configuration());
  }

  /**
   * Creates a {@code Source<GenericData.Record>} by reading the schema of the Avro file
   * at the given path using the {@code FileSystem} information contained in the given
   * {@code Configuration} instance. If the path is a directory, the schema of a file in
   * the directory will be used to determine the schema to use.
   *
   * @param path The path to the data on the filesystem
   * @param conf The configuration information
   * @return A new {@code Source<GenericData.Record>} instance
   */
  public static Source<GenericData.Record> avroFile(Path path, Configuration conf) {
    return avroFile(path, Avros.generics(getSchemaFromPath(path, conf)));
  }

  /**
   * Creates a {@code Source<GenericData.Record>} by reading the schema of the Avro file
   * at the given paths using the {@code FileSystem} information contained in the given
   * {@code Configuration} instance. If the first path is a directory, the schema of a file in
   * the directory will be used to determine the schema to use.
   *
   * @param paths The path to the data on the filesystem
   * @param conf The configuration information
   * @return A new {@code Source<GenericData.Record>} instance
   */
  public static Source<GenericData.Record> avroFile(List<Path> paths, Configuration conf) {
    Preconditions.checkArgument(!paths.isEmpty(), "At least one path must be supplied");
    return avroFile(paths, Avros.generics(getSchemaFromPath(paths.get(0), conf)));
  }

  /**
   * Creates a {@code TableSource<K,V>} for reading an Avro key/value file at the given path.
   *
   * @param path The path to the data on the filesystem
   * @param tableType Avro table type for deserializing the table data
   * @return a new {@code TableSource<K,V>} instance for reading Avro key/value data
   */
  public static <K, V> TableSource<K, V> avroTableFile(Path path, PTableType<K, V> tableType) {
    return avroTableFile(ImmutableList.of(path), tableType);
  }

  /**
   * Creates a {@code TableSource<K,V>} for reading an Avro key/value file at the given paths.
   *
   * @param paths list of paths to be read by the returned source
   * @param tableType Avro table type for deserializing the table data
   * @return a new {@code TableSource<K,V>} instance for reading Avro key/value data
   */
  public static <K, V> TableSource<K, V> avroTableFile(List<Path> paths, PTableType<K, V> tableType) {
    return new AvroTableFileSource<K, V>(paths, (AvroType<Pair<K,V>>)tableType);
  }

  static Schema getSchemaFromPath(Path path, Configuration conf) {
    DataFileReader reader = null;
    try {
      FileSystem fs = path.getFileSystem(conf);

      if (!fs.isFile(path)) {
        PathFilter ignoreHidden = new PathFilter() {
          @Override
          public boolean accept(Path path) {
            String name = path.getName();
            return !name.startsWith("_") && !name.startsWith(".");
          }
        };

        FileStatus[] globStatus = fs.globStatus(path, ignoreHidden);
        if (globStatus == null) {
          throw new IllegalArgumentException("No valid files found in directory: " + path);
        }

        Path newPath = null;
        for (FileStatus status : globStatus) {
          if (status.isFile()) {
              newPath = status.getPath();
              break;
          } else {
            FileStatus[] listStatus = fs.listStatus(path, ignoreHidden);
            if (listStatus != null && listStatus.length > 0) {
                newPath = listStatus[0].getPath();
                break;
            }
          }
        }

        if (newPath == null) {
          throw new IllegalArgumentException("No valid files found in directory: " + path);
        }
        path = newPath;
      }

      reader = new DataFileReader(new FsInput(path, conf), new GenericDatumReader<GenericRecord>());
      return reader.getSchema();
    } catch (IOException e) {
      throw new RuntimeException("Error reading schema from path: "  + path, e);
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          // ignored
        }
      }
    }
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
   * Creates a {@code Source<T>} instance from the SequenceFile(s) at the given {@code Path}s
   * from the value field of each key-value pair in the SequenceFile(s).
   *
   * @param paths A list of {@code Path}s to the data
   * @param valueClass The {@code Writable} type for the value of the SequenceFile entry
   * @return A new {@code Source<T>} instance
   */
  public static <T extends Writable> Source<T> sequenceFile(List<Path> paths, Class<T> valueClass) {
    return sequenceFile(paths, Writables.writables(valueClass));
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
   * Creates a {@code Source<T>} instance from the SequenceFile(s) at the given {@code Path}s
   * from the value field of each key-value pair in the SequenceFile(s).
   *
   * @param paths A list of {@code Path}s to the data
   * @param ptype The {@code PType} for the value of the SequenceFile entry
   * @return A new {@code Source<T>} instance
   */
  public static <T> Source<T> sequenceFile(List<Path> paths, PType<T> ptype) {
    return new SeqFileSource<T>(paths, ptype);
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
   * Creates a {@code TableSource<K, V>} instance for the SequenceFile(s) at the given {@code Path}s.
   *
   * @param paths A list of {@code Path}s to the data
   * @param keyClass The {@code Writable} subclass for the key of the SequenceFile entry
   * @param valueClass The {@code Writable} subclass for the value of the SequenceFile entry
   * @return A new {@code SourceTable<K, V>} instance
   */
  public static <K extends Writable, V extends Writable> TableSource<K, V> sequenceFile(
      List<Path> paths, Class<K> keyClass, Class<V> valueClass) {
    return sequenceFile(paths, Writables.writables(keyClass), Writables.writables(valueClass));
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
   * Creates a {@code TableSource<K, V>} instance for the SequenceFile(s) at the given {@code Path}s.
   *
   * @param paths A list of {@code Path}s to the data
   * @param keyType The {@code PType} for the key of the SequenceFile entry
   * @param valueType The {@code PType} for the value of the SequenceFile entry
   * @return A new {@code SourceTable<K, V>} instance
   */
  public static <K, V> TableSource<K, V> sequenceFile(List<Path> paths, PType<K> keyType, PType<V> valueType) {
    PTypeFamily ptf = keyType.getFamily();
    return new SeqFileTableSource<K, V>(paths, ptf.tableOf(keyType, valueType));
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
   * Creates a {@code Source<String>} instance for the text file(s) at the given {@code Path}s.
   *
   * @param paths A list of {@code Path}s to the data
   * @return A new {@code Source<String>} instance
   */
  public static Source<String> textFile(List<Path> paths) {
    return textFile(paths, Writables.strings());
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

  /**
   * Creates a {@code Source<T>} instance for the text file(s) at the given {@code Path}s using
   * the provided {@code PType<T>} to convert the input text.
   *
   * @param paths A list of {@code Path}s to the data
   * @param ptype The {@code PType<T>} to use to process the input text
   * @return A new {@code Source<T>} instance
   */
  public static <T> Source<T> textFile(List<Path> paths, PType<T> ptype) {
    return new TextFileSource<T>(paths, ptype);
  }
}
