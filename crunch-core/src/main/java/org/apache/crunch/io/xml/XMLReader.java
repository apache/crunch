package org.apache.crunch.io.xml;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

public class XMLReader {
  /**
   * The default input file encoding to read with, UTF-8
   */
  public static final String DEFAULT_INPUT_FILE_ENCODING = "UTF-8";
  /**
   * The default input maximum record size
   */
  public static final int DEFAULT_MAXIMUM_RECORD_SIZE = 67108864;
  /**
   * The default buffer size (64k) to be used when reading from the InputStream
   */
  public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

  private final InputStreamReader inputStreamReader;
  private final CharsetEncoder charsetEncoder;
  private final int maximumRecordSize;
  private final int bufferSize;
  private final String inputFileEncoding;
  private final Pattern openPattern;
  private final Pattern closePattern;

  private String buffer = "";
  private int bufferLength = 0;
  private int bufferPosition = 0;
  private boolean endOfFile = false;

  private boolean inRecord = false;

  public XMLReader(final InputStream inputStream, final Pattern openPattern, final Pattern closePattern,
      final int bufferSize, final String inputFileEncoding, final int maximumRecordSize) {
    this.bufferSize = bufferSize;
    this.inputFileEncoding = inputFileEncoding;
    this.maximumRecordSize = maximumRecordSize;
    this.charsetEncoder = Charset.forName(inputFileEncoding).newEncoder();
    this.openPattern = openPattern;
    this.closePattern = closePattern;

    try {
      this.inputStreamReader = new InputStreamReader(inputStream, inputFileEncoding);
    } catch (final UnsupportedEncodingException uee) {
      throw new RuntimeException(inputFileEncoding + " is not a supported encoding.", uee);
    }
  }

  public int readToNextClose() throws IOException {
    long totalBytesConsumed = 0;
    if (endOfFile) {
      return 0;
    }

    boolean foundCloseTag = false;
    do {
      // Figure out where we are in the buffer and fill it if necessary.
      int startPosition = bufferPosition;
      if (bufferPosition >= bufferLength) {
        refillBuffer();
        startPosition = bufferPosition;
        if (endOfFile) {
          return (int) totalBytesConsumed;
        }
      }

      final Matcher closeMatcher = closePattern.matcher(buffer);
      final boolean foundClose = closeMatcher.find(bufferPosition);

      if (!foundClose) {
        // There isn't a close tag in the buffer
        bufferPosition = bufferLength;
        foundCloseTag = false;
      } else {
        // Found one
        bufferPosition = closeMatcher.end();
        foundCloseTag = true;
      }

      totalBytesConsumed += calculateStringSize(buffer.substring(startPosition, bufferPosition));
    } while (!foundCloseTag && !endOfFile);

    return (int) totalBytesConsumed;
  }

  public int readXMLRecord(final Text input) throws IOException {
    Preconditions.checkNotNull(input, "inputText may not be null");
    long totalBytesConsumed = 0;
    if (endOfFile) {
      return 0;
    }
    if (inRecord) {
      throw new RuntimeException("Cannot begin reading an XML record while inside of a record.");
    }

    boolean foundRecord = false;
    final StringBuilder stringBuilder = new StringBuilder();

    do {
      // Figure out where we are in the buffer and fill it if necessary.
      int startPosition = bufferPosition;
      if (bufferPosition >= bufferLength) {
        refillBuffer();
        startPosition = bufferPosition;
        if (endOfFile) {
          return (int) totalBytesConsumed;
        }
      }

      final Matcher openMatcher = openPattern.matcher(buffer);
      final Matcher closeMatcher = closePattern.matcher(buffer);

      final boolean foundOpen = openMatcher.find(bufferPosition);
      final boolean foundClose = closeMatcher.find(bufferPosition);

      if (!foundOpen && !foundClose) {
        if (inRecord) {
          // the entire buffer is part of the record
          stringBuilder.append(buffer.subSequence(bufferPosition, bufferLength));
          bufferPosition = bufferLength;
          inRecord = true;
          foundRecord = false;
        } else {
          // the entire buffer is junk
          bufferPosition = bufferLength;
          foundRecord = false;
        }
      } else if (!foundOpen && foundClose) {
        if (inRecord) {
          // we are in the middle of a record, but have found the end
          stringBuilder.append(buffer.subSequence(bufferPosition, closeMatcher.end()));
          bufferPosition = closeMatcher.end();
          inRecord = false;
          foundRecord = true;
        } else {
          // We were NOT in a record but still found an ending tag
          throw new RuntimeException("Found unexpected record terminator, file may be malformed.");
        }
      } else if (foundOpen && !foundClose) {
        if (inRecord) {
          // We were already in a record, found the opening of a new record, but
          // not the end of the current one.
          throw new RuntimeException("Found unexpected start of record, file may be malformed.");
        } else {
          // It's possible that there's another open tag that we can see,
          // fail if so.
          final int openPos = openMatcher.start();
          final boolean anotherOpenTag = openMatcher.find();
          if (anotherOpenTag) {
            throw new RuntimeException("Found unexpected start of record, file may be malformed.");
          }
          // we began reading a record, but haven't found the end yet
          stringBuilder.append(buffer.subSequence(openPos, bufferLength));
          bufferPosition = bufferLength;
          inRecord = true;
          foundRecord = false;
        }
      } else if (foundOpen && foundClose) {
        final int openPos = openMatcher.start();
        final int closePos = closeMatcher.start();
        if (inRecord) {
          if (closePos < openPos) {
            // we are in the middle of a record, but have found the end
            stringBuilder.append(buffer.subSequence(bufferPosition, closeMatcher.end()));
            bufferPosition = closeMatcher.end();
            inRecord = false;
            foundRecord = true;
          } else {
            // We are in the middle of a record, but found an open tag before an
            // expected close tag
            throw new RuntimeException("Found unexpected start of record, file may be malformed.");
          }
        } else {
          if (closePos < openPos) {
            // We are not in a record, but found a close tag before the next
            // expected open tag
            throw new RuntimeException("Found unexpected record terminator, file may be malformed.");
          } else {
            // we can see the next whole record in the buffer
            final CharSequence record = buffer.subSequence(openMatcher.start(), closeMatcher.end());
            stringBuilder.append(record);
            bufferPosition = closeMatcher.end();
            inRecord = false;
            foundRecord = true;
          }
        }
      }

      totalBytesConsumed += calculateStringSize(buffer.substring(startPosition, bufferPosition));
      if (totalBytesConsumed >= maximumRecordSize || totalBytesConsumed >= Integer.MAX_VALUE) {
        throw new IOException("Encountered a record greater than the maximum size (" + maximumRecordSize
            + ") or greater than Integer.MAX_VALUE.");
      }

    } while ((inRecord || !foundRecord) && !endOfFile);

    input.set(stringBuilder.toString());
    return (int) totalBytesConsumed;
  }

  private void refillBuffer() throws IOException {
    bufferPosition = 0;
    final char[] tempCharBuffer = new char[this.bufferSize];
    bufferLength = inputStreamReader.read(tempCharBuffer, 0, this.bufferSize);
    final StringBuilder bufferBuilder = new StringBuilder().append(tempCharBuffer);
    buffer = bufferBuilder.toString();

    // If the buffer ends within a tag, we'll expand it a little.
    // NOTE: While it is possible that a file with an uneven set of angle
    // brackets in a CDATA block is being read, I can't think of a situation
    // where reading to the end of the next tag would be harmful.
    final int lastOpen = buffer.lastIndexOf('<');
    final int lastClose = buffer.lastIndexOf('>');
    final boolean bufferEndsInTag = lastOpen > lastClose;
    if (bufferEndsInTag) {
      final char[] readMore = new char[1];
      while (readMore[0] != '>' && inputStreamReader.ready()) {
        bufferLength += inputStreamReader.read(readMore);
        bufferBuilder.append(readMore[0]);
      }
    }

    if (bufferLength <= 0) {
      endOfFile = true;
    }

    buffer = bufferBuilder.toString();
  }

  private int calculateStringSize(final String string) {
    try {
      return charsetEncoder.encode(CharBuffer.wrap(string.toCharArray())).limit();
    } catch (final CharacterCodingException e) {
      throw new RuntimeException("The string attempting to be read (" + string + ") could not be encoded with "
          + inputFileEncoding);
    }
  }
}
