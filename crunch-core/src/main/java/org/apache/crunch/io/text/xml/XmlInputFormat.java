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
package org.apache.crunch.io.text.xml;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

/**
 * Reads records that are delimited by a specific begin/end tag.
 * 
 * The {@link XmlInputFormat} extends the Mahout's XmlInputFormat implementation providing encoding support
 */
public class XmlInputFormat extends TextInputFormat {

  private static final Logger log = LoggerFactory.getLogger(XmlInputFormat.class);

  public static final String START_TAG_KEY = "xmlinput.start";
  public static final String END_TAG_KEY = "xmlinput.end";
  public static final String ENCODING = "xml.encoding";

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
    try {
      return new XmlRecordReader((FileSplit) split, context.getConfiguration());
    } catch (IOException ioe) {
      log.warn("Error while creating XmlRecordReader", ioe);
      return null;
    }
  }

  /**
   * XMLRecordReader class to read through a given xml document to output xml blocks as records as specified by the
   * start tag and end tag.
   */
  public static class XmlRecordReader extends RecordReader<LongWritable, Text> {

    private static final String DEFAULT_ENCODING = Charsets.UTF_8.name();

    private final char[] startTag;
    private final char[] endTag;
    private final long start;
    private final long end;

    private LongWritable currentKey;
    private Text currentValue;
    private final DataOutputBuffer outBuffer;
    private final BufferedReader inReader;
    private final OutputStreamWriter outWriter;
    private final String inputEncoding;
    private long readByteCounter;

    private CharsetEncoder charsetEncoder;

    public XmlRecordReader(FileSplit split, Configuration conf) throws IOException {
      inputEncoding = conf.get(ENCODING, DEFAULT_ENCODING);
      startTag = new String(conf.get(START_TAG_KEY).getBytes(inputEncoding), inputEncoding).toCharArray();
      endTag = new String(conf.get(END_TAG_KEY).getBytes(inputEncoding), inputEncoding).toCharArray();

      // open the file and seek to the start of the split
      start = split.getStart();
      end = start + split.getLength();
      Path file = split.getPath();
      FileSystem fs = file.getFileSystem(conf);
      FSDataInputStream fsin = fs.open(split.getPath());
      fsin.seek(start);
      readByteCounter =  start;
      inReader = new BufferedReader(new InputStreamReader(fsin, Charset.forName(inputEncoding)));
      outBuffer = new DataOutputBuffer();
      outWriter = new OutputStreamWriter(outBuffer, inputEncoding);
      
      charsetEncoder = Charset.forName(inputEncoding).newEncoder();
    }

    private boolean next(LongWritable key, Text value) throws IOException {

      if (readByteCounter < end && readUntilMatch(startTag, false)) {
        try {
          outWriter.write(startTag);

          if (readUntilMatch(endTag, true)) {
            key.set(readByteCounter);
            outWriter.flush();
            value.set(toUTF8(outBuffer.getData()), 0, outBuffer.getLength());
            return true;
          }
        } finally {
          outWriter.flush();
          outBuffer.reset();
        }
      }
      return false;
    }

    private byte[] toUTF8(byte[] in) throws UnsupportedEncodingException {
      return new String(in, inputEncoding).getBytes(Charsets.UTF_8);
    }

    @Override
    public void close() throws IOException {
      inReader.close();
    }

    @Override
    public float getProgress() throws IOException {
      return (readByteCounter - start) / (float) (end - start);
    }

    private boolean readUntilMatch(char[] match, boolean withinBlock) throws IOException {
      int i = 0;
      while (true) {
        int nextInCharacter = inReader.read();

        readByteCounter = readByteCounter + calculateCharacterByteLength((char) nextInCharacter);

        // end of file:
        if (nextInCharacter == -1) {
          return false;
        }
        // save to buffer:
        if (withinBlock) {
          outWriter.write(nextInCharacter);
        }

        // check if we're matching:
        if (nextInCharacter == match[i]) {
          i++;
          if (i >= match.length) {
            return true;
          }
        } else {
          i = 0;
        }
        // see if we've passed the stop point
        if (!withinBlock && i == 0 && readByteCounter >= end) {
          return false;
        }
      }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
      return currentKey;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
      return currentValue;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      currentKey = new LongWritable();
      currentValue = new Text();
      return next(currentKey, currentValue);
    }
    
    private int calculateCharacterByteLength(final char character) {
      try {
        return charsetEncoder.encode(CharBuffer.wrap(new char[] { character })).limit();
      } catch (final CharacterCodingException e) {
        throw new RuntimeException("The character attempting to be read (" + character + ") could not be encoded with "
            + inputEncoding);
      }
    }
  }
}
