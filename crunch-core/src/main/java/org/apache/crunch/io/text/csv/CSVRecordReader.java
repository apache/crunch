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
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * An extension of {@link RecordReader} used to intelligently read CSV files
 */
public class CSVRecordReader extends RecordReader<LongWritable, Text> {
  private static final Logger LOGGER = LoggerFactory.getLogger(CSVRecordReader.class);
  private long start;
  private long pos;
  private long end;
  private LongWritable key = null;
  private Text value = null;

  private InputStream fileIn;
  private CSVLineReader csvLineReader;
  private final char openQuote;
  private final char closeQuote;
  private final char escape;
  private final String inputFileEncoding;
  private final int fileStreamBufferSize;
  private final int maximumRecordSize;

  private int totalRecordsRead = 0;

  /**
   * Default constructor, specifies default values for the {@link CSVLineReader}
   */
  public CSVRecordReader() {
    this(CSVLineReader.DEFAULT_BUFFER_SIZE, CSVLineReader.DEFAULT_INPUT_FILE_ENCODING,
        CSVLineReader.DEFAULT_QUOTE_CHARACTER, CSVLineReader.DEFAULT_QUOTE_CHARACTER,
        CSVLineReader.DEFAULT_ESCAPE_CHARACTER, CSVLineReader.DEFAULT_MAXIMUM_RECORD_SIZE);
  }

  /**
   * Customizable constructor used to specify all input parameters for the
   * {@link CSVLineReader}
   * 
   * @param bufferSize
   *          the size of the buffer to use while parsing through the input file
   * @param inputFileEncoding
   *          the encoding for the input file
   * @param openQuote
   *          the character to use to open quote blocks
   * @param closeQuote
   *          the character to use to close quote blocks
   * @param escape
   *          the character to use for escaping control characters and quotes
   * @param maximumRecordSize
   *          The maximum acceptable size of one CSV record. Beyond this limit,
   *          {@code CSVLineReader} will stop parsing and an exception will be
   *          thrown.
   */
  public CSVRecordReader(final int bufferSize, final String inputFileEncoding, final char openQuote,
      final char closeQuote, final char escape, final int maximumRecordSize) {
    Preconditions.checkNotNull(openQuote, "quote cannot be null.");
    Preconditions.checkNotNull(closeQuote, "quote cannot be null.");
    Preconditions.checkNotNull(escape, "escape cannot be null.");

    this.fileStreamBufferSize = bufferSize;
    this.inputFileEncoding = inputFileEncoding;
    this.openQuote = openQuote;
    this.closeQuote = closeQuote;
    this.escape = escape;
    this.maximumRecordSize = maximumRecordSize;
  }

  /**
   * Initializes the record reader
   * 
   * @param genericSplit
   *          the split assigned to this record reader
   * @param context
   *          the job context for this record reader
   * @throws IOException
   *           if an IOException occurs while handling the file to be read
   */
  @Override
  public void initialize(final InputSplit genericSplit, final TaskAttemptContext context) throws IOException {
    final FileSplit split = (FileSplit) genericSplit;
    final Configuration job = context.getConfiguration();

    start = split.getStart();
    end = start + split.getLength();
    this.pos = start;

    final Path file = split.getPath();

    CompressionCodecFactory codecFactory = new CompressionCodecFactory(context.getConfiguration());
    CompressionCodec compressionCodec = codecFactory.getCodec(file);

    LOGGER.info("Initializing processing of split for file: {}", file);
    LOGGER.info("File size is: {}", file.getFileSystem(job).getFileStatus(file).getLen());
    LOGGER.info("Split starts at: {}", start);
    LOGGER.info("Split will end at: {}", end);
    LOGGER.info("File is compressed: {}", (compressionCodec != null));

    // Open the file, seek to the start of the split
    // then wrap it in a CSVLineReader
    if(compressionCodec == null) {
      FSDataInputStream in = file.getFileSystem(job).open(file);
      in.seek(start);
      fileIn = in;
    }else{
      fileIn = compressionCodec.createInputStream(file.getFileSystem(job).open(file));
    }
    csvLineReader = new CSVLineReader(fileIn, this.fileStreamBufferSize, inputFileEncoding, this.openQuote,
        this.closeQuote, this.escape, this.maximumRecordSize);
  }

  /**
   * Increments the key and value pair for this reader
   * 
   * @return true if there is another key/value to be read, false if not.
   * @throws IOException
   *           if an IOException occurs while handling the file to be read
   */
  @Override
  public boolean nextKeyValue() throws IOException {
    if (key == null) {
      key = new LongWritable();
    }
    key.set(pos);
    if (value == null) {
      value = new Text();
    }

    if (pos >= end) {
      key = null;
      value = null;
      LOGGER.info("End of split reached, ending processing. Total records read for this split: {}", totalRecordsRead);
      close();
      return false;
    }

    final int newSize = csvLineReader.readCSVLine(value);

    if (newSize == 0) {
      LOGGER.info("End of file reached. Ending processing. Total records read for this split: {}", totalRecordsRead);
      return false;
    }

    pos += newSize;
    totalRecordsRead++;
    return true;
  }

  /**
   * Returns the current key
   * 
   * @return the key corresponding to the current value
   */
  @Override
  public LongWritable getCurrentKey() {
    return key;
  }

  /**
   * Returns the current value
   * 
   * @return the value corresponding to the current key
   */
  @Override
  public Text getCurrentValue() {
    return value;
  }

  /**
   * Get the progress within the split
   */
  @Override
  public float getProgress() {
    if (start == end) {
      return 0.0f;
    }
    return Math.min(1.0f, (pos - start) / (float) (end - start));
  }

  /**
   * Closes the file input stream for this record reader
   */
  @Override
  public synchronized void close() throws IOException {
    fileIn.close();
  }
}