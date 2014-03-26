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
import java.io.InputStream;
import java.util.ArrayList;

import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.junit.Rule;
import org.junit.Test;

public class CSVRecordIteratorTest {
  @Rule
  public transient TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testVanillaCSV() throws IOException {
    String[] expectedFileContents = { "1,2,3,4", "5,6,7,8", "9,10,11", "12,13,14" };

    String vanillaCSVFile = tmpDir.copyResourceFileName("vanilla.csv");
    File vanillaFile = new File(vanillaCSVFile);
    InputStream inputStream = new FileInputStream(vanillaFile);
    CSVRecordIterator csvRecordIterator = new CSVRecordIterator(inputStream);

    ArrayList<String> fileContents = new ArrayList<String>(5);
    while (csvRecordIterator.hasNext()) {
      fileContents.add(csvRecordIterator.next());
    }

    for (int i = 0; i < expectedFileContents.length; i++) {
      assertEquals(expectedFileContents[i], fileContents.get(i));
    }
  }

  @Test
  public void testCSVWithNewlines() throws IOException {
    String[] expectedFileContents = {
        "\"Champion, Mac\",\"1234 Hoth St.\n\tApartment 101\n\tAtlanta, GA\n\t64086\",\"30\",\"M\",\"5/28/2010 12:00:00 AM\",\"Just some guy\"",
        "\"Champion, Mac\",\"5678 Tatooine Rd. Apt 5, Mobile, AL 36608\",\"30\",\"M\",\"Some other date\",\"short description\"" };
    String csvWithNewlines = tmpDir.copyResourceFileName("withNewlines.csv");
    File fileWithNewlines = new File(csvWithNewlines);
    InputStream inputStream = new FileInputStream(fileWithNewlines);
    CSVRecordIterator csvRecordIterator = new CSVRecordIterator(inputStream);

    ArrayList<String> fileContents = new ArrayList<String>(2);
    while (csvRecordIterator.hasNext()) {
      fileContents.add(csvRecordIterator.next());
    }

    for (int i = 0; i < expectedFileContents.length; i++) {
      assertEquals(expectedFileContents[i], fileContents.get(i));
    }
  }
}
