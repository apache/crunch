/*
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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.crunch.io.avro;

import java.util.List;

import org.apache.crunch.Pair;
import org.apache.crunch.TableSource;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.avro.AvroType;
import org.apache.hadoop.fs.Path;

/**
 * A file source for reading a table of Avro keys and values.
 *
 * This file source can be used for reading and writing tables compatible with
 * the {@code org.apache.avro.mapred.AvroJob} and {@code org.apache.avro.mapreduce.AvroJob} classes (in addition to
 * tables created by Crunch).
 *
 * @see org.apache.crunch.types.avro.Avros#tableOf(org.apache.crunch.types.PType, org.apache.crunch.types.PType)
 * @see org.apache.crunch.types.avro.Avros#keyValueTableOf(org.apache.crunch.types.PType, org.apache.crunch.types.PType)
 */
public class AvroTableFileSource<K, V> extends AvroFileSource<Pair<K, V>> implements TableSource<K,V> {

  public AvroTableFileSource(List<Path> paths, AvroType<Pair<K, V>> tableType) {
    super(paths, tableType);
  }

  @Override
  public PTableType<K, V> getTableType() {
    return (PTableType<K,V>)super.getType();
  }
}
