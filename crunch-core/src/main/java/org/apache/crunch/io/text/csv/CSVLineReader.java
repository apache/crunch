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
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * A record reader written specifically to read individual lines from CSV files.
 * Most notably, it can read CSV records which span multiple lines.
 */
@ParametersAreNonnullByDefault
public class CSVLineReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(CSVLineReader.class);

  // InputStream related variables
  /**
   * The default buffer size (64k) to be used when reading from the InputStream
   */
  public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
  private final InputStreamReader inputStreamReader;
  private final String inputFileEncoding;
  private final CharsetEncoder charsetEncoder;
  private char[] buffer;
  private final int bufferSize;
  private int bufferLength = 0;
  private int bufferPosition = 0;
  private boolean bufferIsPadded = false;
  private static char CR = '\r';
  private static char LF = '\n';
  private boolean endOfFile = false;

  // CSV parsing related variables
  /**
   * The default character to represent quotation marks, '"'
   */
  public static final char DEFAULT_QUOTE_CHARACTER = '"';
  /**
   * The default character to represent an escape used before a control
   * character that should be displayed, '\'
   */
  public static final char DEFAULT_ESCAPE_CHARACTER = '\\';
  /**
   * The default character to represent a null character, '\0'
   */
  public static final char NULL_CHARACTER = '\0';
  /**
   * The default input file encoding to read with, UTF-8
   */
  public static final String DEFAULT_INPUT_FILE_ENCODING = "UTF-8";
  /**
   * The default input maximum record size
   */
  public static final int DEFAULT_MAXIMUM_RECORD_SIZE = 67108864;

  private final int maximumRecordSize;
  private final char openQuoteChar;
  private final char closeQuoteChar;
  private final char escape;
  private boolean inMultiLine = false;
  private boolean currentlyInQuotes = false;
  private boolean endOfLineReached = false;
  private Text inputText = new Text();

  /**
   * This constructor will use default values for buffer size and control
   * characters.
   * 
   * @param inputStream
   *          The @{link InputStream} to read from. Note that this input stream
   *          should start at the very beginning of the CSV file to be read OR
   *          at the very beginning of a CSV entry. If the input stream starts
   *          at any other position (such as in the middle of a line) this
   *          reader will not work properly.
   * @throws UnsupportedEncodingException
   */
  public CSVLineReader(final InputStream inputStream) throws UnsupportedEncodingException {
    this(inputStream, DEFAULT_BUFFER_SIZE, DEFAULT_INPUT_FILE_ENCODING, DEFAULT_QUOTE_CHARACTER,
        DEFAULT_QUOTE_CHARACTER, DEFAULT_ESCAPE_CHARACTER, DEFAULT_MAXIMUM_RECORD_SIZE);
  }

  /**
   * The fully customizable constructor for CSVLineReader
   * 
   * @param inputStream
   *          The @{link InputStream} to read from. Note that this input stream
   *          should start at the very beginning of the CSV file to be read OR
   *          at the very beginning of a CSV entry. If the input stream starts
   *          at any other position (such as in the middle of a line) this
   *          reader will not work properly.
   * @param bufferSize
   *          The size of the buffer used when reading the input stream
   * @param inputFileEncoding
   *          The encoding of the file to read from.
   * @param openQuoteChar
   *          Used to specify a custom open quote character
   * @param closeQuoteChar
   *          Used to specify a custom close quote character
   * @param escapeChar
   *          Used to specify a custom escape character
   * @param maximumRecordSize
   *          The maximum acceptable size of one CSV record. Beyond this limit,
   *          parsing will stop and an exception will be thrown.
   * @throws UnsupportedEncodingException
   */
  public CSVLineReader(final InputStream inputStream, final int bufferSize, final String inputFileEncoding,
      final char openQuoteChar, final char closeQuoteChar, final char escapeChar, final int maximumRecordSize) {
    Preconditions.checkNotNull(inputStream, "inputStream may not be null");
    Preconditions.checkNotNull(inputFileEncoding, "inputFileEncoding may not be null");
    if (bufferSize <= 0) {
      throw new IllegalArgumentException("The buffer (" + bufferSize + ")cannot be <= 0");
    }

    // Input Stream related variables
    try {
      this.inputStreamReader = new InputStreamReader(inputStream, inputFileEncoding);
    } catch (final UnsupportedEncodingException uee) {
      throw new RuntimeException(inputFileEncoding + " is not a supported encoding.", uee);
    }
    this.bufferSize = bufferSize;
    this.buffer = new char[this.bufferSize];

    // CSV parsing related variables
    if (isSameCharacter(openQuoteChar, escapeChar)) {
      throw new IllegalArgumentException("The open quote (" + openQuoteChar + ") and escape (" + escapeChar
          + ") characters must be different!");
    }
    if (isSameCharacter(closeQuoteChar, escapeChar)) {
      throw new IllegalArgumentException("The close quote (" + closeQuoteChar + ") and escape (" + escapeChar
          + ") characters must be different!");
    }
    this.openQuoteChar = openQuoteChar;
    this.closeQuoteChar = closeQuoteChar;
    this.escape = escapeChar;
    this.inputFileEncoding = inputFileEncoding;
    this.charsetEncoder = Charset.forName(inputFileEncoding).newEncoder();
    this.maximumRecordSize = maximumRecordSize;
  }

  /**
   * This method will read through one full CSV record, place its content into
   * the input Text and return the number of bytes (including newline
   * characters) that were consumed.
   * 
   * @param input
   *          a mutable @{link Text} object into which the text of the CSV
   *          record will be stored, without any line feeds or carriage returns
   * @return the number of byes that were read, including any control
   *         characters, line feeds, or carriage returns.
   * @throws IOException
   *           if an IOException occurs while handling the file to be read
   */
  public int readCSVLine(final Text input) throws IOException {
    Preconditions.checkNotNull(input, "inputText may not be null");
    inputText = new Text(input);
    long totalBytesConsumed = 0;
    if (endOfFile) {
      return 0;
    }
    if (inMultiLine) {
      throw new RuntimeException("Cannot begin reading a CSV record while inside of a multi-line CSV record.");
    }

    final StringBuilder stringBuilder = new StringBuilder();
    do {
      // Read a line from the file and add it to the builder
      inputText.clear();
      totalBytesConsumed += readFileLine(inputText);
      stringBuilder.append(inputText.toString());

      if (currentlyInQuotes && !endOfFile) {
        // If we end up in a multi-line record, we need append a newline
        stringBuilder.append('\n');

        // Do a check on the total bytes consumed to see if something has gone
        // wrong.
        if (totalBytesConsumed > maximumRecordSize || totalBytesConsumed > Integer.MAX_VALUE) {
          final String record = stringBuilder.toString();
          LOGGER.error("Possibly malformed file encountered. First line of record: {}",
               record.substring(0, record.indexOf('\n')));
          throw new IOException("Possibly malformed file encountered. Check log statements for more information");
        }
      }
    } while (currentlyInQuotes && !endOfFile);

    // Set the input to the multi-line record
    input.set(stringBuilder.toString());
    return (int) totalBytesConsumed;
  }

  /**
   * A method for reading through one single line in the CSV file, that is, it
   * will read until the first line feed, carriage return, or set of both is
   * found. The CSV parsing logic markers are maintained outside of this method
   * to enable the manipulation that logic in order to find the beginning of a
   * CSV record. Use {@link CSVLineReader#isInMultiLine()} and
   * {@link CSVLineReader#resetMultiLine()} to do so. See
   * {@link CSVInputFormat#getSplitsForFile(long, long, org.apache.hadoop.fs.Path, org.apache.hadoop.fs.FSDataInputStream)}
   * for an example.
   * 
   * @param input
   *          a mutable @{link Text} object into which the text of the line will
   *          be stored, without any line feeds or carriage returns
   * @return the number of byes that were read, including any control
   *         characters, line feeds, or carriage returns.
   * @throws IOException
   *           if an IOException occurs while handling the file to be read
   */
  public int readFileLine(final Text input) throws IOException {
    Preconditions.checkNotNull(input, "inputText may not be null");
    if (endOfFile) {
      return 0;
    }

    // This integer keeps track of the number of newline characters used to
    // terminate the line being read. This could be 1, in the case of LF or CR,
    // or 2, in the case of CRLF.
    int newlineLength = 0;
    int inputTextLength = 0;
    long bytesConsumed = 0;
    int readTextLength = 0;
    int startPosition = bufferPosition;
    endOfLineReached = false;
    inputText = new Text(input);

    do {
      boolean checkForLF = false;
      // Figure out where we are in the buffer and fill it if necessary.
      if (bufferPosition >= bufferLength) {
        refillBuffer();
        startPosition = bufferPosition;
        if (endOfFile) {
          break;
        }
      }

      newlineLength = 0;
      // Iterate through the buffer looking for newline characters while keeping
      // track of if we're in a field and/or in quotes.
      for (; bufferPosition < bufferLength; ++bufferPosition) {
        bytesConsumed += calculateCharacterByteLength(buffer[bufferPosition]);
        if (buffer[bufferPosition] == this.escape) {
          if (isNextCharacterEscapable(currentlyInQuotes, bufferPosition)) {
            // checks to see if we are in quotes and if the next character is a
            // quote or an escape
            // character. If so, that's fine. Record the next character's size
            // and skip it.
            ++bufferPosition;
            bytesConsumed += calculateCharacterByteLength(buffer[bufferPosition]);
          }
        } else if (buffer[bufferPosition] == openQuoteChar || buffer[bufferPosition] == closeQuoteChar) {
          // toggle currentlyInQuotes if we've hit a non-escaped quote character
          currentlyInQuotes = !currentlyInQuotes;
        } else if (buffer[bufferPosition] == LF || buffer[bufferPosition] == CR) {
          boolean lastCharWasCR = buffer[bufferPosition] == CR;
          // Line is over, make note and increment the size of the newlinelength
          // counter.
          endOfLineReached = true;
          ++newlineLength;
          ++bufferPosition;
          if (lastCharWasCR && buffer[bufferPosition] == LF) {
            lastCharWasCR = false;
            // Check for LF (in case of CRLF line endings) and increment the
            // counter, skip it by moving the buffer position, then record the
            // length of the LF.
            ++newlineLength;
            ++bufferPosition;
            bytesConsumed += calculateCharacterByteLength(buffer[bufferPosition]);
          } else if (lastCharWasCR && bufferPosition >= bufferLength) {
            // We just read a CR at the very end of the buffer. If this is a
            // file with CRLF line endings, there will be a LF next that we need
            // to check for and account for in bytesRead before we count this
            // line as "read".
            checkForLF = true;
          }
          break;
        }
      }
      // This is the length of the actual text and important stuff in the line.
      readTextLength = bufferPosition - startPosition - newlineLength;

      // Append the results.
      if (readTextLength > Integer.MAX_VALUE - inputTextLength) {
        readTextLength = Integer.MAX_VALUE - inputTextLength;
      }
      if (readTextLength > 0) {
        // This will append the portion of the buffer containing only the
        // important text, omitting any newline characters
        inputText.set(new StringBuilder().append(inputText.toString())
            .append(new String(buffer, startPosition, readTextLength)).toString());
        inputTextLength += readTextLength;
      }

      // If the last character we read was a CR at the end of the buffer, we
      // need to check for an LF after a buffer refill.
      if (checkForLF) {
        refillBuffer();
        if (endOfFile) {
          break;
        }
        if (buffer[bufferPosition] == LF) {
          bytesConsumed += calculateCharacterByteLength(buffer[bufferPosition]);
          ++bufferPosition;
          ++newlineLength;
        }
      }

    } while (newlineLength == 0 && bytesConsumed < Integer.MAX_VALUE);

    if (endOfLineReached) {
      if (currentlyInQuotes) {
        inMultiLine = true;
      } else {
        inMultiLine = false;
      }
    }

    if (bytesConsumed > Integer.MAX_VALUE) {
      throw new IOException("Too many bytes consumed before newline: " + Integer.MAX_VALUE);
    }

    input.set(inputText);
    return (int) bytesConsumed;
  }

  /**
   * For use with {@link CSVLineReader#readFileLine(Text)}. Returns current
   * multi-line CSV status.
   * 
   * @return a boolean signifying if the last
   *         {@link CSVLineReader#readFileLine(Text)} call ended in the middle
   *         of a multi-line CSV record
   */
  public boolean isInMultiLine() {
    return inMultiLine;
  }

  /**
   * For use with {@link CSVLineReader#readFileLine(Text)}. Resets current
   * multi-line CSV status.
   */
  public void resetMultiLine() {
    inMultiLine = false;
    currentlyInQuotes = false;
  }

  private boolean isSameCharacter(final char c1, final char c2) {
    return c1 != NULL_CHARACTER && c1 == c2;
  }

  private boolean isNextCharacterEscapable(final boolean inQuotes, final int i) {
    return inQuotes // we are in quotes, therefore there can be escaped quotes
                    // in here.
        && buffer.length > (i + 1) // there is indeed another character to
                                   // check.
        && (buffer[i + 1] == closeQuoteChar || buffer[i + 1] == openQuoteChar || buffer[i + 1] == this.escape);
  }

  private void refillBuffer() throws IOException {
    bufferPosition = 0;

    // Undo the buffer padding
    if (bufferIsPadded) {
      buffer = new char[bufferLength];
      bufferIsPadded = false;
    }

    bufferLength = inputStreamReader.read(buffer, 0, buffer.length);
    // if bufferLength < bufferSize, this buffer will contain the end of the
    // file. However, our line logic needs to be able to see what's a few spots
    // past the current position. This will cause an index out of bounds
    // exception if the buffer is full. So, my solution is to add a few extra
    // spaces to the buffer so that the logic can still read ahead.
    if (buffer.length == bufferLength) {
      final char[] biggerBuffer = new char[bufferLength + 3];
      for (int i = 0; i < bufferLength; i++) {
        biggerBuffer[i] = buffer[i];
      }
      buffer = biggerBuffer;
      bufferIsPadded = true;
    }

    if (bufferLength <= 0) {
      endOfFile = true;
    }
  }

  private int calculateCharacterByteLength(final char character) {
    try {
      return charsetEncoder.encode(CharBuffer.wrap(new char[] { character })).limit();
    } catch (final CharacterCodingException e) {
      throw new RuntimeException("The character attempting to be read (" + character + ") could not be encoded with "
          + inputFileEncoding);
    }
  }
}