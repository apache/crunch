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

import java.util.List;

import org.apache.crunch.io.FileReaderFactory;
import org.apache.crunch.io.impl.ReadableDataImpl;
import org.apache.hadoop.fs.Path;

public class CSVReadableData extends ReadableDataImpl<String> {

  private final int bufferSize;
  private final String inputFileEncoding;
  private final char openQuoteChar;
  private final char closeQuoteChar;
  private final char escapeChar;
  private final int maximumRecordSize;

  /**
   * Creates an instance of {@code CSVReadableData} with default configuration
   * 
   * @param paths
   *          The paths of the files to be read
   */
  protected CSVReadableData(final List<Path> paths) {
    this(paths, CSVLineReader.DEFAULT_BUFFER_SIZE, CSVLineReader.DEFAULT_INPUT_FILE_ENCODING,
        CSVLineReader.DEFAULT_QUOTE_CHARACTER, CSVLineReader.DEFAULT_QUOTE_CHARACTER,
        CSVLineReader.DEFAULT_ESCAPE_CHARACTER, CSVLineReader.DEFAULT_MAXIMUM_RECORD_SIZE);
  }

  /**
   * Creates an instance of {@code CSVReadableData} with specified configuration
   * 
   * @param paths
   *          a list of input file paths
   * @param bufferSize
   *          the size of the buffer to use while parsing through the input file
   * @param inputFileEncoding
   *          the encoding for the input file
   * @param openQuoteChar
   *          the character to use to open quote blocks
   * @param closeQuoteChar
   *          the character to use to close quote blocks
   * @param escapeChar
   *          the character to use for escaping control characters and quotes
   * @param maximumRecordSize
   *          The maximum acceptable size of one CSV record. Beyond this limit,
   *          {@code CSVLineReader} will stop parsing and an exception will be
   *          thrown.
   */
  protected CSVReadableData(final List<Path> paths, final int bufferSize, final String inputFileEncoding,
      final char openQuoteChar, final char closeQuoteChar, final char escapeChar, final int maximumRecordSize) {
    super(paths);
    this.bufferSize = bufferSize;
    this.inputFileEncoding = inputFileEncoding;
    this.openQuoteChar = openQuoteChar;
    this.closeQuoteChar = closeQuoteChar;
    this.escapeChar = escapeChar;
    this.maximumRecordSize = maximumRecordSize;
  }

  @Override
  protected FileReaderFactory<String> getFileReaderFactory() {
    return new CSVFileReaderFactory(bufferSize, inputFileEncoding, openQuoteChar, closeQuoteChar, escapeChar,
        maximumRecordSize);
  }
}