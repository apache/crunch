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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.crunch.io.text.xml.XmlInputFormat.XmlRecordReader;
import org.apache.crunch.test.TemporaryPath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * {@link XmlRecordReader} Test.
 *
 */
public class XmlRecordReaderTest {

  @Rule
  public transient TemporaryPath tmpDir = new TemporaryPath(RuntimeParameters.TMP_DIR, "hadoop.tmp.dir");

  private Configuration conf;

  private String xmlFile;

  private long xmlFileLength;

  @Before
  public void before() throws IOException {
    xmlFile = tmpDir.copyResourceFileName("xmlSourceSample3.xml");
    xmlFileLength = getFileLength(xmlFile);

    conf = new org.apache.hadoop.conf.Configuration();
    conf.set(XmlInputFormat.START_TAG_KEY, "<PLANT");
    conf.set(XmlInputFormat.END_TAG_KEY, "</PLANT>");
  }

  @Test
  public void testStartOffsets() throws Exception {
    /*
     * The xmlSourceSample3.xml file byte ranges:
     *
     * 50-252 - first PLANT element.
     *
     * 254-454 - second PLANT element.
     *
     * 456-658 - third PLANT element.
     */
    assertEquals("Starting from offset 0 should read all elements", 3, readXmlElements(createSplit(0, xmlFileLength)));
    assertEquals("Offset is in the middle of the first element. Should read only the remaining 2 elements", 2,
        readXmlElements(createSplit(100, xmlFileLength)));
    assertEquals("Offset is in the middle of the second element. Should read only the remaining 1 element", 1,
        readXmlElements(createSplit(300, xmlFileLength)));
    assertEquals("Offset is in the middle of the third element. Should read no elements", 0,
        readXmlElements(createSplit(500, xmlFileLength)));
  }

  @Test
  public void readThroughSplitEnd() throws IOException, InterruptedException {
    // Third element starts at position: 456 and has length: 202
    assertEquals("Split starts before the 3rd element and ends in the middle of the 3rd element.", 1,
        readXmlElements(createSplit(300, ((456 - 300) + 202 / 2))));
    assertEquals("Split starts and ends before the 3rd element.", 0, readXmlElements(createSplit(300, (456 - 300))));
  }

  private FileSplit createSplit(long offset, long length) {
    return new FileSplit(new Path(xmlFile), offset, length, new String[] {});
  }

  private long readXmlElements(FileSplit split) throws IOException, InterruptedException {

    int elementCount = 0;

    XmlRecordReader xmlRecordReader = new XmlRecordReader(split, conf);
    try {
      long lastKey = 0;
      while (xmlRecordReader.nextKeyValue()) {
        elementCount++;
        assertTrue(xmlRecordReader.getCurrentKey().get() > lastKey);
        lastKey = xmlRecordReader.getCurrentKey().get();
        assertTrue(xmlRecordReader.getCurrentValue().getLength() > 0);
      }
    } finally {
      xmlRecordReader.close();
    }

    return elementCount;
  }

  private long getFileLength(String fileName) throws FileNotFoundException, IOException {
    return new File(fileName).length();
  }
}
