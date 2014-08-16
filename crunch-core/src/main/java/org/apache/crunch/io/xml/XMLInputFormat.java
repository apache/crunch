package org.apache.crunch.io.xml;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * A {@link FileInputFormat} for use specifically with XML files.
 */
public class XMLInputFormat extends FileInputFormat<LongWritable, Text> implements Configurable {
  private int bufferSize;
  private String inputFileEncoding;
  private int maximumRecordSize;
  private Configuration configuration;
  private Pattern openPattern;
  private Pattern closePattern;

  @Override
  public List<InputSplit> getSplits(final JobContext job) throws IOException {
    final long splitSize = job.getConfiguration().getLong(XMLFileSource.INPUT_SPLIT_SIZE, 67108864);
    final List<InputSplit> splits = new ArrayList<InputSplit>();
    final Path[] paths = FileUtil.stat2Paths(listStatus(job).toArray(new FileStatus[0]));
    final FileSystem fileSystem = FileSystem.get(job.getConfiguration());
    FSDataInputStream inputStream = null;
    try {
      for (final Path path : paths) {
        inputStream = fileSystem.open(path);
        splits.addAll(getSplitsForFile(splitSize, fileSystem.getFileStatus(path).getLen(), path, inputStream));
      }
      return splits;
    } finally {
      if (inputStream != null) {
        inputStream.close();
      }
    }
  }

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
        final FileSplit fileSplit = new FileSplit(fileName, splitStart, currentPosition - splitStart, new String[] {});
        splitsList.add(fileSplit);
        break;
      }

      // Every time we seek to the new approximate split point,
      // we need to create a new XMLReader around the stream.
      inputStream.seek(currentPosition);
      final XMLReader xmlReader = new XMLReader(inputStream, this.openPattern, this.closePattern, this.bufferSize,
          this.inputFileEncoding, this.maximumRecordSize);

      // Find the next close tag
      currentPosition += xmlReader.readToNextClose();

      splitsList.add(new FileSplit(fileName, splitStart, currentPosition - splitStart, new String[] {}));
    }

    return splitsList;
  }

  /**
   * This method will read the configuration options that were set in
   * {@link XMLFileSource}'s private getBundle() method
   */
  public void configure() {
    bufferSize = this.configuration.getInt(XMLFileSource.BUFFER_SIZE, -1);
    if (bufferSize < 0) {
      bufferSize = XMLReader.DEFAULT_BUFFER_SIZE;
    }

    final String inputFileEncodingValue = this.configuration.get(XMLFileSource.INPUT_FILE_ENCODING);
    if ("".equals(inputFileEncodingValue)) {
      inputFileEncoding = XMLReader.DEFAULT_INPUT_FILE_ENCODING;
    } else {
      inputFileEncoding = inputFileEncodingValue;
    }

    maximumRecordSize = this.configuration.getInt(XMLFileSource.MAXIMUM_RECORD_SIZE, -1);
    if (maximumRecordSize < 0) {
      maximumRecordSize = this.configuration.getInt(XMLFileSource.INPUT_SPLIT_SIZE,
          XMLReader.DEFAULT_MAXIMUM_RECORD_SIZE);
    }

    final String openPatternValue = this.configuration.get(XMLFileSource.OPEN_PATTERN);
    if ("".equals(openPatternValue)) {
      throw new RuntimeException("Configured value for openPattern was not found.");
    } else {
      openPattern = Pattern.compile(openPatternValue);
    }

    final String closePatternValue = this.configuration.get(XMLFileSource.CLOSE_PATTERN);
    if ("".equals(closePatternValue)) {
      throw new RuntimeException("Configured value for closePattern was not found.");
    } else {
      closePattern = Pattern.compile(closePatternValue);
    }
  }

  @Override
  public Configuration getConf() {
    return configuration;
  }

  @Override
  public void setConf(final Configuration configuration) {
    this.configuration = configuration;
    configure();
  }

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(final InputSplit arg0, final TaskAttemptContext arg1)
      throws IOException, InterruptedException {
    return new XMLRecordReader(openPattern, closePattern, bufferSize, inputFileEncoding, maximumRecordSize);
  }
}
