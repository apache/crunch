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
package org.apache.crunch.types;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.crunch.DoFn;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.io.ReadableSourceTarget;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * A {@code PType} defines a mapping between a data type that is used in a Crunch pipeline and a
 * serialization and storage format that is used to read/write data from/to HDFS. Every
 * {@link PCollection} has an associated {@code PType} that tells Crunch how to read/write data from
 * that {@code PCollection}.
 * 
 */
public interface PType<T> extends Serializable {
  /**
   * Returns the Java type represented by this {@code PType}.
   */
  Class<T> getTypeClass();

  /**
   * Returns the {@code PTypeFamily} that this {@code PType} belongs to.
   */
  PTypeFamily getFamily();

  MapFn<Object, T> getInputMapFn();

  MapFn<T, Object> getOutputMapFn();

  Converter getConverter();

  /**
   * Initialize this PType for use within a DoFn. This generally only needs to be called when using
   * a PType for {@link #getDetachedValue(Object)}.
   * 
   * @param conf Configuration object
   * @see PType#getDetachedValue(Object)
   */
  void initialize(Configuration conf);

  /**
   * Returns a copy of a value (or the value itself) that can safely be retained.
   * <p>
   * This is useful when iterable values being processed in a DoFn (via a reducer) need to be held
   * on to for more than the scope of a single iteration, as a reducer (and therefore also a DoFn
   * that has an Iterable as input) re-use deserialized values. More information on object reuse is
   * available in the {@link DoFn} class documentation.
   * 
   * @param value The value to be deep-copied
   * @return A deep copy of the input value
   */
  T getDetachedValue(T value);

  /**
   * Returns a {@code SourceTarget} that is able to read/write data using the serialization format
   * specified by this {@code PType}.
   */
  ReadableSourceTarget<T> getDefaultFileSource(Path path);

  /**
   * Returns a {@code ReadableSource} that contains the data in the given {@code Iterable}.
   *
   * @param conf The Configuration to use
   * @param path The path to write the data to
   * @param contents The contents to write
   * @param parallelism The desired parallelism
   * @return A new instance of ReadableSource
   */
  ReadableSource<T> createSourceTarget(Configuration conf, Path path, Iterable<T> contents, int parallelism)
    throws IOException;

  /**
   * Returns the sub-types that make up this PType if it is a composite instance, such as a tuple.
   */
  List<PType> getSubTypes();
}
