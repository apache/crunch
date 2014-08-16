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
import java.util.List;

import org.apache.crunch.ReadableData;
import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.io.impl.FileSourceImpl;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * A {@code Source} instance that uses the {@code CSVInputFormat}, which gives
 * each map task one single CSV record, regardless of how many lines it may
 * span.
 */
public class CSVFileSource extends FileSourceImpl<String> implements ReadableSource<String> {

  /**
   * The key used in the {@code CSVInputFormat}'s {@code FormatBundle} to set
   * the underlying {@code CSVLineReader}'s buffer size
   */
  public static final String CSV_BUFFER_SIZE = "csv.buffersize";

  /**
   * The key used in the {@code CSVInputFormat}'s {@code FormatBundle} to set
   * the underlying {@code CSVLineReader}'s input file encoding
   */
  public static final String CSV_INPUT_FILE_ENCODING = "csv.inputfileencoding";

  /**
   * The key used in the {@code CSVInputFormat}'s {@code FormatBundle} to set
   * the underlying {@code CSVLineReader}'s open quote character
   */
  public static final String CSV_OPEN_QUOTE_CHAR = "csv.openquotechar";

  /**
   * The key used in the {@code CSVInputFormat}'s {@code FormatBundle} to set
   * the underlying {@code CSVLineReader}'s close quote character
   */
  public static final String CSV_CLOSE_QUOTE_CHAR = "csv.closequotechar";

  /**
   * The key used in the {@code CSVInputFormat}'s {@code FormatBundle} to set
   * the underlying {@code CSVLineReader}'s escape character
   */
  public static final String CSV_ESCAPE_CHAR = "csv.escapechar";

  /**
   * The key used in the {@code CSVInputFormat}'s {@code FormatBundle} to set
   * the underlying {@code CSVLineReader}'s maximum record size. If this is not
   * set, INPUT_SPLIT_SIZE will be checked first, and if that is not set, 64mb
   * will be assumed.
   */
  public static final String MAXIMUM_RECORD_SIZE = "csv.maximumrecordsize";

  /**
   * The key used in the {@code CSVInputFormat}'s {@code FormatBundle} to set
   * the underlying {@code CSVLineReader}'s input split size. If it is not set,
   * 64mb will be assumed.
   */
  public static final String INPUT_SPLIT_SIZE = "csv.inputsplitsize";

  private int bufferSize;
  private String inputFileEncoding;
  private char openQuoteChar;
  private char closeQuoteChar;
  private char escapeChar;
  private int maximumRecordSize;

  /**
   * Create a new CSVFileSource instance
   * 
   * @param paths
   *          The {@code Path} to the input data
   */
  public CSVFileSource(final List<Path> paths) {
    this(paths, CSVLineReader.DEFAULT_BUFFER_SIZE, CSVLineReader.DEFAULT_INPUT_FILE_ENCODING,
        CSVLineReader.DEFAULT_QUOTE_CHARACTER, CSVLineReader.DEFAULT_QUOTE_CHARACTER,
        CSVLineReader.DEFAULT_ESCAPE_CHARACTER, CSVLineReader.DEFAULT_MAXIMUM_RECORD_SIZE);
  }

  /**
   * Create a new CSVFileSource instance
   * 
   * @param path
   *          The {@code Path} to the input data
   */
  public CSVFileSource(final Path path) {
    this(path, CSVLineReader.DEFAULT_BUFFER_SIZE, CSVLineReader.DEFAULT_INPUT_FILE_ENCODING,
        CSVLineReader.DEFAULT_QUOTE_CHARACTER, CSVLineReader.DEFAULT_QUOTE_CHARACTER,
        CSVLineReader.DEFAULT_ESCAPE_CHARACTER, CSVLineReader.DEFAULT_MAXIMUM_RECORD_SIZE);
  }

  /**
   * Create a new CSVFileSource instance with all configurable options.
   * 
   * @param paths
   *          A list of {@code Path}s to be used as input data.
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
  public CSVFileSource(final List<Path> paths, final int bufferSize, final String inputFileEncoding,
      final char openQuoteChar, final char closeQuoteChar, final char escapeChar, final int maximumRecordSize) {
    super(paths, Writables.strings(), getCSVBundle(bufferSize, inputFileEncoding, openQuoteChar, closeQuoteChar,
        escapeChar, maximumRecordSize));
    setPrivateVariables(bufferSize, inputFileEncoding, openQuoteChar, closeQuoteChar, escapeChar, maximumRecordSize);
  }

  /**
   * Create a new CSVFileSource instance with all configurable options.
   * 
   * @param path
   *          The {@code Path} to the input data
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
  public CSVFileSource(final Path path, final int bufferSize, final String inputFileEncoding, final char openQuoteChar,
      final char closeQuoteChar, final char escapeChar, final int maximumRecordSize) {
    super(path, Writables.strings(), getCSVBundle(bufferSize, inputFileEncoding, openQuoteChar, closeQuoteChar,
        escapeChar, maximumRecordSize));
    setPrivateVariables(bufferSize, inputFileEncoding, openQuoteChar, closeQuoteChar, escapeChar, maximumRecordSize);
  }

  @Override
  public Iterable<String> read(final Configuration conf) throws IOException {
    return read(conf, new CSVFileReaderFactory(bufferSize, inputFileEncoding, openQuoteChar, closeQuoteChar,
        escapeChar, maximumRecordSize));
  }

  @Override
  public ReadableData<String> asReadable() {
    return new CSVReadableData(paths, bufferSize, inputFileEncoding, openQuoteChar, closeQuoteChar, escapeChar,
        maximumRecordSize);
  }

  @Override
  public String toString() {
    return "CSV(" + pathsAsString() + ")";
  }

  /**
   * Configures the job with any custom options. These will be retrieved later
   * by {@code CSVInputFormat}
   */
  private static FormatBundle<CSVInputFormat> getCSVBundle(final int bufferSize, final String inputFileEncoding,
      final char openQuoteChar, final char closeQuoteChar, final char escapeChar, final int maximumRecordSize) {
    final FormatBundle<CSVInputFormat> bundle = FormatBundle.forInput(CSVInputFormat.class);
    bundle.set(RuntimeParameters.DISABLE_COMBINE_FILE, "true");
    bundle.set(CSV_BUFFER_SIZE, String.valueOf(bufferSize));
    bundle.set(CSV_INPUT_FILE_ENCODING, String.valueOf(inputFileEncoding));
    bundle.set(CSV_OPEN_QUOTE_CHAR, String.valueOf(openQuoteChar));
    bundle.set(CSV_CLOSE_QUOTE_CHAR, String.valueOf(closeQuoteChar));
    bundle.set(CSV_ESCAPE_CHAR, String.valueOf(escapeChar));
    bundle.set(MAXIMUM_RECORD_SIZE, String.valueOf(maximumRecordSize));
    return bundle;
  }

  private void setPrivateVariables(final int bufferSize, final String inputFileEncoding, final char openQuoteChar,
      final char closeQuoteChar, final char escapeChar, final int maximumRecordSize) {
    if (isSameCharacter(openQuoteChar, escapeChar)) {
      throw new IllegalArgumentException("The open quote (" + openQuoteChar + ") and escape (" + escapeChar
          + ") characters must be different!");
    }
    if (isSameCharacter(closeQuoteChar, escapeChar)) {
      throw new IllegalArgumentException("The close quote (" + closeQuoteChar + ") and escape (" + escapeChar
          + ") characters must be different!");
    }
    this.bufferSize = bufferSize;
    this.inputFileEncoding = inputFileEncoding;
    this.openQuoteChar = openQuoteChar;
    this.closeQuoteChar = closeQuoteChar;
    this.escapeChar = escapeChar;
    this.maximumRecordSize = maximumRecordSize;
  }

  private boolean isSameCharacter(final char c1, final char c2) {
    return c2 == c1;
  }
}