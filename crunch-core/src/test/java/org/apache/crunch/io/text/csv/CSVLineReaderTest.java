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

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.hadoop.io.Text;
import org.junit.Rule;
import org.junit.Test;

public class CSVLineReaderTest {
  @Rule
  public transient TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testVariousUTF8Characters() throws IOException {
    final String variousCharacters = "€Abبώиב¥£€¢₡₢₣₤₥₦§₧₨₩₪₫₭₮漢Ä©óíßä";
    String utf8Junk = tmpDir.copyResourceFileName("UTF8.csv");
    FileInputStream fileInputStream = null;
    try {

      fileInputStream = new FileInputStream(new File(utf8Junk));
      final CSVLineReader csvLineReader = new CSVLineReader(fileInputStream);
      final Text readText = new Text();
      csvLineReader.readCSVLine(readText);
      assertEquals(variousCharacters, readText.toString());
    } finally {
      fileInputStream.close();
    }
  }

  /**
   * This is effectively a mirror the address tests, but using Chinese
   * characters, even for the quotation marks and escape characters.
   * 
   * @throws IOException
   */
  @Test
  public void testBrokenLineParsingInChinese() throws IOException {
    final String[] expectedChineseLines = { "您好我叫马克，我从亚拉巴马州来，我是软件工程师，我二十八岁", "我有一个宠物，它是一个小猫，它六岁，它很漂亮",
        "我喜欢吃饭，“我觉得这个饭最好\n＊蛋糕\n＊包子\n＊冰淇淋\n＊啤酒“，他们都很好，我也很喜欢奶酪但它是不健康的", "我是男的，我的头发很短，我穿蓝色的裤子，“我穿黑色的、“衣服”" };
    String chineseLines = tmpDir.copyResourceFileName("brokenChineseLines.csv");
    FileInputStream fileInputStream = null;
    try {
      fileInputStream = new FileInputStream(new File(chineseLines));
      final CSVLineReader csvLineReader = new CSVLineReader(fileInputStream, CSVLineReader.DEFAULT_BUFFER_SIZE,
          CSVLineReader.DEFAULT_INPUT_FILE_ENCODING, '“', '”', '、');
      for (int i = 0; i < expectedChineseLines.length; i++) {
        final Text readText = new Text();
        csvLineReader.readCSVLine(readText);
        assertEquals(expectedChineseLines[i], readText.toString());
      }
    } finally {
      fileInputStream.close();
    }
  }
}