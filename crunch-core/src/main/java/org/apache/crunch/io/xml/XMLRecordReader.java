package org.apache.crunch.io.xml;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XMLRecordReader extends RecordReader<LongWritable, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(XMLRecordReader.class);
  private long start;
  private long pos;
  private long end;
  private long totalRecordsRead = 0;
  private LongWritable key = new LongWritable();
  private Text value = new Text();
  private FSDataInputStream fileIn;
  private XMLReader xmlReader;

  private final String inputFileEncoding;
  private final int fileStreamBufferSize;
  private final int maximumRecordSize;
  private final Pattern openPattern;
  private final Pattern closePattern;

  public XMLRecordReader(final Pattern openPattern, final Pattern closePattern, final int bufferSize,
      final String inputFileEncoding, final int maximumRecordSize) {
    this.fileStreamBufferSize = bufferSize;
    this.inputFileEncoding = inputFileEncoding;
    this.maximumRecordSize = maximumRecordSize;
    this.openPattern = openPattern;
    this.closePattern = closePattern;
  }

  @Override
  public void initialize(final InputSplit genericSplit, final TaskAttemptContext context) throws IOException {
    final FileSplit split = (FileSplit) genericSplit;
    final Configuration job = context.getConfiguration();

    start = split.getStart();
    end = start + split.getLength();
    this.pos = start;

    final Path file = split.getPath();
    LOG.info("Initializing processing of split for file: " + file);
    LOG.info("File size is: " + file.getFileSystem(job).getFileStatus(file).getLen());
    LOG.info("Split starts at: " + start);
    LOG.info("Split will end at: " + end);

    // Open the file, seek to the start of the split
    // then wrap it in a CSVLineReader
    fileIn = file.getFileSystem(job).open(file);
    fileIn.seek(start);
    xmlReader = new XMLReader(fileIn, this.openPattern, this.closePattern, this.fileStreamBufferSize,
        inputFileEncoding, this.maximumRecordSize);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
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
      LOG.info("End of split reached, ending processing. Total records read for this split: " + totalRecordsRead);
      close();
      return false;
    }

    final int newSize = xmlReader.readXMLRecord(value);

    if (newSize == 0) {
      LOG.info("End of file reached. Ending processing. Total records read for this split: " + totalRecordsRead);
      return false;
    }

    pos += newSize;
    totalRecordsRead++;
    return true;
  }

  @Override
  public void close() throws IOException {
    fileIn.close();
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (start == end) {
      return 0.0f;
    }
    return Math.min(1.0f, (pos - start) / (float) (end - start));
  }

}
