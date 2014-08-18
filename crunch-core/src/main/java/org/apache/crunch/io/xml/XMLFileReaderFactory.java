package org.apache.crunch.io.xml;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.crunch.io.FileReaderFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Iterators;

public class XMLFileReaderFactory implements FileReaderFactory<String> {
  private static final Log LOG = LogFactory.getLog(XMLFileReaderFactory.class);

  private final int maximumRecordSize;
  private final int bufferSize;
  private final String inputFileEncoding;
  private final Pattern openPattern;
  private final Pattern closePattern;

  public XMLFileReaderFactory(final Pattern openPattern, final Pattern closePattern, final int bufferSize,
      final String inputFileEncoding, final int maximumRecordSize) {
    this.bufferSize = bufferSize;
    this.inputFileEncoding = inputFileEncoding;
    this.maximumRecordSize = maximumRecordSize;
    this.openPattern = openPattern;
    this.closePattern = closePattern;
  }

  @Override
  public Iterator<String> read(final FileSystem fs, final Path path) {
    FSDataInputStream is;
    try {
      is = fs.open(path);
      return new XMLRecordIterator(is, openPattern, closePattern, bufferSize, inputFileEncoding, maximumRecordSize);
    } catch (final IOException e) {
      LOG.info("Could not read path: " + path, e);
      return Iterators.emptyIterator();
    }
  }
}
