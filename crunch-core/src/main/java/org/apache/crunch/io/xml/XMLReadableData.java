package org.apache.crunch.io.xml;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.crunch.io.FileReaderFactory;
import org.apache.crunch.io.impl.ReadableDataImpl;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XMLReadableData extends ReadableDataImpl<String> {
  private static final Logger LOG = LoggerFactory.getLogger(XMLReadableData.class);
  private final String inputFileEncoding;
  private final int maximumRecordSize;
  private final int bufferSize;
  private final Pattern openPattern;
  private final Pattern closePattern;

  protected XMLReadableData(final List<Path> paths, final int bufferSize, final String inputFileEncoding,
      final Pattern openPattern, final Pattern closePattern, final int maximumRecordSize) {
    super(paths);
    this.bufferSize = bufferSize;
    this.inputFileEncoding = inputFileEncoding;
    this.openPattern = openPattern;
    this.closePattern = closePattern;
    this.maximumRecordSize = maximumRecordSize;
  }

  @Override
  protected FileReaderFactory<String> getFileReaderFactory() {
    return new XMLFileReaderFactory(openPattern, closePattern, bufferSize, inputFileEncoding, maximumRecordSize);
  }

}
