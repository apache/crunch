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
package org.apache.crunch.io.text.csv;

import java.io.IOException;
import java.util.Iterator;

import org.apache.crunch.io.FileReaderFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code FileReaderFactory} instance that is responsible for building a
 * {@code CSVRecordIterator}
 */
public class CSVFileReaderFactory implements FileReaderFactory<String> {
  private static final Logger LOG = LoggerFactory.getLogger(CSVFileReaderFactory.class);
  private final int bufferSize;
  private final String inputFileEncoding;
  private final char openQuoteChar;
  private final char closeQuoteChar;
  private final char escapeChar;
  private final int maximumRecordSize;

  /**
   * Creates a new {@code CSVFileReaderFactory} instance with default
   * configuration
   */
  CSVFileReaderFactory() {
    this(CSVLineReader.DEFAULT_BUFFER_SIZE, CSVLineReader.DEFAULT_INPUT_FILE_ENCODING,
        CSVLineReader.DEFAULT_QUOTE_CHARACTER, CSVLineReader.DEFAULT_QUOTE_CHARACTER,
        CSVLineReader.DEFAULT_ESCAPE_CHARACTER, CSVLineReader.DEFAULT_MAXIMUM_RECORD_SIZE);
  }

  /**
   * Creates a new {@code CSVFileReaderFactory} instance with custom
   * configuration
   * 
   * @param bufferSize
   *          The size of the buffer to be used in the underlying
   *          {@code CSVLineReader}
   * @param inputFileEncoding
   *          The the encoding of the input file to be read by the underlying
   *          {@code CSVLineReader}
   * @param openQuoteChar
   *          The character representing the quote character to be used in the
   *          underlying {@code CSVLineReader}
   * @param closeQuoteChar
   *          The character representing the quote character to be used in the
   *          underlying {@code CSVLineReader}
   * @param escapeChar
   *          The character representing the escape character to be used in the
   *          underlying {@code CSVLineReader}
   * @param maximumRecordSize
   *          The maximum acceptable size of one CSV record. Beyond this limit,
   *          {@code CSVLineReader} will stop parsing and an exception will be
   *          thrown.
   */
  CSVFileReaderFactory(final int bufferSize, final String inputFileEncoding, final char openQuoteChar,
      final char closeQuoteChar, final char escapeChar, final int maximumRecordSize) {
    this.bufferSize = bufferSize;
    this.inputFileEncoding = inputFileEncoding;
    this.openQuoteChar = openQuoteChar;
    this.closeQuoteChar = closeQuoteChar;
    this.escapeChar = escapeChar;
    this.maximumRecordSize = maximumRecordSize;
  }

  @Override
  public Iterator<String> read(final FileSystem fs, final Path path) {
    FSDataInputStream is;
    try {
      is = fs.open(path);
      return new CSVRecordIterator(is, bufferSize, inputFileEncoding, openQuoteChar, closeQuoteChar, escapeChar,
          maximumRecordSize);
    } catch (final IOException e) {
      LOG.info("Could not read path: {}", path, e);
      return Iterators.emptyIterator();
    }
  }
}