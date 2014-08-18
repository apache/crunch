package org.apache.crunch.io.xml;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;

import com.google.common.io.Closeables;

public class XMLRecordIterator implements Iterator<String>, Closeable {
  private final XMLReader xmlReader;
  private InputStream inputStream;
  private String currentRecord;

  public XMLRecordIterator(final InputStream inputStream, final Pattern openPattern, final Pattern closePattern,
      final int bufferSize, final String inputFileEncoding, final int maximumRecordSize) {
    xmlReader = new XMLReader(inputStream, openPattern, closePattern, bufferSize, inputFileEncoding, maximumRecordSize);
    this.inputStream = inputStream;
    incrementValue();
  }

  private void incrementValue() {
    final Text tempText = new Text();
    try {
      xmlReader.readXMLRecord(tempText);
    } catch (final IOException e) {
      throw new RuntimeException("A problem occurred accessing the underlying CSV file stream.", e);
    }
    final String tempTextAsString = tempText.toString();
    if ("".equals(tempTextAsString)) {
      currentRecord = null;
    } else {
      currentRecord = tempTextAsString;
    }
  }

  @Override
  public void close() throws IOException {
    if (inputStream != null) {
      inputStream.close();
      inputStream = null;
    }
  }

  @Override
  public boolean hasNext() {
    if (!(currentRecord == null)) {
      return true;
    }
    Closeables.closeQuietly(this);
    return false;
  }

  @Override
  public String next() {
    final String result = currentRecord;
    incrementValue();
    return result;
  }

  @Override
  public void remove() {
    incrementValue();
  }

}
