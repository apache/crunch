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

import org.apache.crunch.ReadableData;
import org.apache.crunch.Source;
import org.apache.hadoop.conf.Configuration;

/**
 * An extension of the {@code Source} interface that indicates that a
 * {@code Source} instance may be read as a series of records by the client
 * code. This is used to determine whether a {@code PCollection} instance can be
 * materialized.
 */
public interface ReadableSource<T> extends Source<T> {

  /**
   * Returns an {@code Iterable} that contains the contents of this source.
   * 
   * @param conf The current {@code Configuration} instance
   * @return the contents of this {@code Source} as an {@code Iterable} instance
   * @throws IOException
   */
  Iterable<T> read(Configuration conf) throws IOException;

  /**
   * @return a {@code ReadableData} instance containing the data referenced by this
   * {@code ReadableSource}.
   */
  ReadableData<T> asReadable();
}
