/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.crunch.io.text.csv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.google.common.annotations.VisibleForTesting;

/**
 * A {@link FileInputFormat} for use specifically with CSV files. This input
 * format deals with the fact that CSV files can potentially have multiple lines
 * within fields which should all be treated as one record.
 */
public class CSVInputFormat extends FileInputFormat<LongWritable, Text> implements Configurable {
  @VisibleForTesting
  protected int bufferSize;
  @VisibleForTesting
  protected String inputFileEncoding;
  @VisibleForTesting
  protected char openQuoteChar;
  @VisibleForTesting
  protected char closeQuoteChar;
  @VisibleForTesting
  protected char escapeChar;
  @VisibleForTesting
  protected int maximumRecordSize;
  private Configuration configuration;

  /**
   * This method is used by crunch to get an instance of {@link CSVRecordReader}
   *
   * @param split
   *          the {@link InputSplit} that will be assigned to the record reader
   * @param context
   *          the {@TaskAttemptContext} for the job
   * @return an instance of {@link CSVRecordReader} created using configured
   *         separator, quote, escape, and maximum record size.
   */
  @Override
  public RecordReader<LongWritable, Text> createRecordReader(final InputSplit split, final TaskAttemptContext context) {
    return new CSVRecordReader(this.bufferSize, this.inputFileEncoding, this.openQuoteChar, this.closeQuoteChar,
        this.escapeChar, this.maximumRecordSize);
  }

  /**
   * A method used by crunch to calculate the splits for each file. This will
   * split each CSV file at the end of a valid CSV record. The default split
   * size is 64mb, but this can be reconfigured by setting the
   * "csv.inputsplitsize" option in the job configuration.
   *
   * @param job
   *          the {@link JobContext} for the current job.
   * @return a List containing all of the calculated splits for a single file.
   * @throws IOException
   *           if an error occurs while accessing HDFS
   */
  @Override
  public List<InputSplit> getSplits(final JobContext job) throws IOException {
    final long splitSize = job.getConfiguration().getLong(CSVFileSource.INPUT_SPLIT_SIZE, 67108864);
    final List<InputSplit> splits = new ArrayList<InputSplit>();
    final Path[] paths = FileUtil.stat2Paths(listStatus(job).toArray(new FileStatus[0]));
    FSDataInputStream inputStream = null;

    Configuration config = job.getConfiguration();
    CompressionCodecFactory compressionCodecFactory = new CompressionCodecFactory(config);

    try {
      for (final Path path : paths) {
        FileSystem fileSystem = path.getFileSystem(config);
        CompressionCodec codec = compressionCodecFactory.getCodec(path);
        if(codec == null) {
          //if file is not compressed then split it up.
          inputStream = fileSystem.open(path);
          splits.addAll(getSplitsForFile(splitSize, fileSystem.getFileStatus(path).getLen(), path, inputStream));
        }else{
          //compressed file so no splitting it
          splits.add(new FileSplit(path,0, Long.MAX_VALUE, new String[0]));
        }
      }
      return splits;
    } finally {
      if (inputStream != null) {
        inputStream.close();
      }
    }
  }

  @Override
  public Configuration getConf() {
    return configuration;
  }

  @Override
  public void setConf(final Configuration conf) {
    configuration = conf;
    configure();
  }

  /**
   * This method will read the configuration options that were set in
   * {@link CSVFileSource}'s private getBundle() method
   */
  public void configure() {
    inputFileEncoding = this.configuration.get(CSVFileSource.CSV_INPUT_FILE_ENCODING, CSVLineReader.DEFAULT_INPUT_FILE_ENCODING);
    maximumRecordSize = this.configuration.getInt(CSVFileSource.MAXIMUM_RECORD_SIZE, this.configuration.getInt(CSVFileSource.INPUT_SPLIT_SIZE, CSVLineReader.DEFAULT_MAXIMUM_RECORD_SIZE));
    closeQuoteChar = this.configuration.get(CSVFileSource.CSV_CLOSE_QUOTE_CHAR, String.valueOf(CSVLineReader.DEFAULT_QUOTE_CHARACTER)).charAt(0);
    openQuoteChar = this.configuration.get(CSVFileSource.CSV_OPEN_QUOTE_CHAR, String.valueOf(CSVLineReader.DEFAULT_QUOTE_CHARACTER)).charAt(0);
    escapeChar = this.configuration.get(CSVFileSource.CSV_ESCAPE_CHAR, String.valueOf(CSVLineReader.DEFAULT_ESCAPE_CHARACTER)).charAt(0);
    bufferSize = this.configuration.getInt(CSVFileSource.CSV_BUFFER_SIZE, CSVLineReader.DEFAULT_BUFFER_SIZE);
  }

  /**
   * In summary, this method will start at the beginning of the file, seek to
   * the position corresponding to the desired split size, seek to the end of
   * the line that contains that position, then attempt to seek until the
   * CSVLineReader indicates that the current position is no longer within a CSV
   * record. Then, it will mark that position for a split and a repeat its
   * logic.
   */
  @VisibleForTesting
  protected List<FileSplit> getSplitsForFile(final long splitSize, final long fileSize, final Path fileName,
                                             final FSDataInputStream inputStream) throws IOException {
    final List<FileSplit> splitsList = new ArrayList<FileSplit>();

    long splitStart;
    long currentPosition = 0;

    boolean endOfFile = false;
    while (!endOfFile) {
      // Set the start of this split to the furthest read point in the file
      splitStart = currentPosition;

      // Skip a number of bytes equal to the desired split size to avoid parsing
      // every csv line, which greatly increases the run time
      currentPosition = splitStart + splitSize;

      // The input stream will freak out if we try to seek past the EOF
      if (currentPosition >= fileSize) {
        currentPosition = fileSize;
        endOfFile = true;
        final FileSplit fileSplit = new FileSplit(fileName, splitStart, currentPosition - splitStart, new String[]{});
        splitsList.add(fileSplit);
        break;
      }

      // Every time we seek to the new approximate split point,
      // we need to create a new CSVLineReader around the stream.
      inputStream.seek(currentPosition);
      final CSVLineReader csvLineReader = new CSVLineReader(inputStream, this.bufferSize, this.inputFileEncoding,
          this.openQuoteChar, this.closeQuoteChar, this.escapeChar, this.maximumRecordSize);

      // This line is potentially garbage because we most likely just sought to
      // the middle of a line. Read the rest of the line and leave it for the
      // previous split. Then reset the multi-line CSV record boolean, because
      // the partial line will have a very high chance of falsely triggering the
      // class-wide multi-line logic.
      currentPosition += csvLineReader.readFileLine(new Text());
      csvLineReader.resetMultiLine();

      // Now, we may still be in the middle of a multi-line CSV record.
      currentPosition += csvLineReader.readFileLine(new Text());

      // If we are, read until we are not.
      while (csvLineReader.isInMultiLine()) {
        final int bytesRead = csvLineReader.readFileLine(new Text());
        // End of file
        if (bytesRead <= 0) {
          break;
        }
        currentPosition += bytesRead;
      }

      // We're out of the multi-line CSV record, so it's safe to end the
      // previous split.
      splitsList.add(new FileSplit(fileName, splitStart, currentPosition - splitStart, new String[]{}));
    }

    return splitsList;
  }

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    return codec == null;
  }
}
