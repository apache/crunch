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

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Matchers;
import org.mockito.Mockito;

public class CSVInputFormatTest {

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Test
  public void testNullConfiguration() {
    final CSVInputFormat csvInputFormat = new CSVInputFormat();
    final Configuration mockedConfiguration = Mockito.mock(Configuration.class);
    csvInputFormat.setConf(mockedConfiguration);
    csvInputFormat.configure();
      
    Assert.assertEquals(CSVLineReader.DEFAULT_BUFFER_SIZE, csvInputFormat.bufferSize);
    Assert.assertEquals(CSVLineReader.DEFAULT_BUFFER_SIZE, csvInputFormat.bufferSize);
    Assert.assertEquals(CSVLineReader.DEFAULT_INPUT_FILE_ENCODING, csvInputFormat.inputFileEncoding);
    Assert.assertEquals(CSVLineReader.DEFAULT_QUOTE_CHARACTER, csvInputFormat.openQuoteChar);
    Assert.assertEquals(CSVLineReader.DEFAULT_QUOTE_CHARACTER, csvInputFormat.closeQuoteChar);
    Assert.assertEquals(CSVLineReader.DEFAULT_ESCAPE_CHARACTER, csvInputFormat.escapeChar);
    Assert.assertEquals(CSVLineReader.DEFAULT_MAXIMUM_RECORD_SIZE, csvInputFormat.maximumRecordSize);
  }

  @Test
  public void testBlankConfiguration() {
    final CSVInputFormat csvInputFormat = new CSVInputFormat();
    final Configuration mockedConfiguration = Mockito.mock(Configuration.class);
    Mockito.doReturn("").when(mockedConfiguration).get(Matchers.anyString());

    csvInputFormat.setConf(mockedConfiguration);
    csvInputFormat.configure();
    Assert.assertEquals(CSVLineReader.DEFAULT_BUFFER_SIZE, csvInputFormat.bufferSize);
    Assert.assertEquals(CSVLineReader.DEFAULT_INPUT_FILE_ENCODING, csvInputFormat.inputFileEncoding);
    Assert.assertEquals(CSVLineReader.DEFAULT_QUOTE_CHARACTER, csvInputFormat.openQuoteChar);
    Assert.assertEquals(CSVLineReader.DEFAULT_QUOTE_CHARACTER, csvInputFormat.closeQuoteChar);
    Assert.assertEquals(CSVLineReader.DEFAULT_ESCAPE_CHARACTER, csvInputFormat.escapeChar);
    Assert.assertEquals(CSVLineReader.DEFAULT_MAXIMUM_RECORD_SIZE, csvInputFormat.maximumRecordSize);
  }

  @Test
  public void testReasonableConfiguration() {
    final CSVInputFormat csvInputFormat = new CSVInputFormat();
    final Configuration mockedConfiguration = Mockito.mock(Configuration.class);
    Mockito.doReturn("UTF8").when(mockedConfiguration).get(CSVFileSource.CSV_INPUT_FILE_ENCODING);
    Mockito.doReturn("O").when(mockedConfiguration).get(CSVFileSource.CSV_OPEN_QUOTE_CHAR);
    Mockito.doReturn("C").when(mockedConfiguration).get(CSVFileSource.CSV_CLOSE_QUOTE_CHAR);
    Mockito.doReturn("E").when(mockedConfiguration).get(CSVFileSource.CSV_ESCAPE_CHAR);
    Mockito.doReturn(10001).when(mockedConfiguration).getInt(CSVFileSource.MAXIMUM_RECORD_SIZE, -1);
    Mockito.doReturn(1000).when(mockedConfiguration).getInt(CSVFileSource.CSV_BUFFER_SIZE, -1);

    csvInputFormat.setConf(mockedConfiguration);
    csvInputFormat.configure();
    Assert.assertEquals(1000, csvInputFormat.bufferSize);
    Assert.assertEquals("UTF8", csvInputFormat.inputFileEncoding);
    Assert.assertEquals('O', csvInputFormat.openQuoteChar);
    Assert.assertEquals('C', csvInputFormat.closeQuoteChar);
    Assert.assertEquals('E', csvInputFormat.escapeChar);
    Assert.assertEquals(10001, csvInputFormat.maximumRecordSize);
  }
}