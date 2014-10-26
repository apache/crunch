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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Iterator;

import org.apache.crunch.io.FileReaderFactory;
import org.apache.crunch.io.impl.AutoClosingIterator;
import org.apache.crunch.types.PType;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TextFileReaderFactory<T> implements FileReaderFactory<T> {

  private static final Logger LOG = LoggerFactory.getLogger(TextFileReaderFactory.class);

  private final LineParser<T> parser;

  public TextFileReaderFactory(PType<T> ptype) {
    this(LineParser.forType(ptype));
  }
  
  public TextFileReaderFactory(LineParser<T> parser) {
    this.parser = parser;
  }

  @Override
  public Iterator<T> read(FileSystem fs, Path path) {
    parser.initialize();

    FSDataInputStream is;
    try {
      is = fs.open(path);
    } catch (IOException e) {
      LOG.info("Could not read path: {}", path, e);
      return Iterators.emptyIterator();
    }

    final BufferedReader reader = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
    return new AutoClosingIterator<T>(reader, new UnmodifiableIterator<T>() {
      boolean nextChecked = false;
      private String nextLine;

      @Override
      public boolean hasNext() {
        if (nextChecked) {
          return nextLine != null;
        }

        try {
          nextChecked = true;
          return (nextLine = reader.readLine()) != null;
        } catch (IOException e) {
          LOG.info("Exception reading text file stream", e);
          return false;
        }
      }

      @Override
      public T next() {
        if (!nextChecked && !hasNext()) {
          return null;
        }
        nextChecked = false;
        return parser.parse(nextLine);
      }
    });
  }
}
