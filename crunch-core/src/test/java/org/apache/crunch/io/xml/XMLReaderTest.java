package org.apache.crunch.io.xml;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.hadoop.io.Text;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class XMLReaderTest {
  @Rule
  public transient TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testOneFullyReadRecordWithBufferThrashing() throws IOException {
    final String waffles = "<food>\n\t\t<name>Belgian Waffles</name>\n\t\t<price>$5.95</price>\n\t\t<description>Two of our famous Belgian Waffles with plenty of real maple syrup</description>\n\t\t<calories>650</calories>\n\t</food>";
    final String simpleXML = tmpDir.copyResourceFileName("simple.xml");

    FileInputStream fileInputStream = null;
    try {
      fileInputStream = new FileInputStream(new File(simpleXML));
      final XMLReader xmlReader = new XMLReader(fileInputStream, Pattern.compile("<food>"), Pattern.compile("</food>"),
          3, "UTF-8", 500);
      final Text readText = new Text();
      xmlReader.readXMLRecord(readText);
      assertEquals(waffles, readText.toString());
    } finally {
      fileInputStream.close();
    }
  }

  @Test
  public void testReadAllRecords() throws IOException {
    final String simpleXML = tmpDir.copyResourceFileName("simple.xml");

    FileInputStream fileInputStream = null;
    try {
      fileInputStream = new FileInputStream(new File(simpleXML));
      final XMLReader xmlReader = new XMLReader(fileInputStream, Pattern.compile("<food>"), Pattern.compile("</food>"),
          500, "UTF-8", 500);
      final Text readText = new Text();

      int i = 1;
      final ImmutableSet.Builder<String> recordsBuilder = ImmutableSet.builder();
      while (i != 0) {
        i = xmlReader.readXMLRecord(readText);
        recordsBuilder.add(readText.toString());
      }
      assertEquals(5, recordsBuilder.build().size());

    } finally {
      fileInputStream.close();
    }
  }

  @Test(expected = IOException.class)
  public void testRecordTooBig() throws IOException {
    final String waffles = "<food>\n\t\t<name>Belgian Waffles</name>\n\t\t<price>$5.95</price>\n\t\t<description>Two of our famous Belgian Waffles with plenty of real maple syrup</description>\n\t\t<calories>650</calories>\n\t</food>";
    final String simpleXML = tmpDir.copyResourceFileName("simple.xml");

    FileInputStream fileInputStream = null;
    try {
      fileInputStream = new FileInputStream(new File(simpleXML));
      final XMLReader xmlReader = new XMLReader(fileInputStream, Pattern.compile("<food>"), Pattern.compile("</food>"),
          100, "UTF-8", 5);
      final Text readText = new Text();
      xmlReader.readXMLRecord(readText);
      assertEquals(waffles, readText.toString());
    } finally {
      fileInputStream.close();
    }
  }

  @Test
  public void testReadToNextClose() throws IOException {
    final String simpleXML = tmpDir.copyResourceFileName("simple.xml");

    FileInputStream fileInputStream = null;
    try {
      fileInputStream = new FileInputStream(new File(simpleXML));
      final XMLReader xmlReader = new XMLReader(fileInputStream, Pattern.compile("<food>"), Pattern.compile("</food>"),
          5, "UTF-8", 500);
      final int bytesRead = xmlReader.readToNextClose();
      assertEquals(274, bytesRead);
    } finally {
      fileInputStream.close();
    }
  }

  @Test
  public void testReadToNextCloseAtEOF() throws IOException {
    final String simpleXML = tmpDir.copyResourceFileName("simple.xml");

    FileInputStream fileInputStream = null;
    try {
      fileInputStream = new FileInputStream(new File(simpleXML));
      fileInputStream.skip(1076);
      final XMLReader xmlReader = new XMLReader(fileInputStream, Pattern.compile("<food>"), Pattern.compile("</food>"),
          5, "UTF-8", 500);
      final int bytesRead = xmlReader.readToNextClose();
      assertEquals(18, bytesRead);
    } finally {
      fileInputStream.close();
    }
  }

  @Test
  public void testReadAllRecordsRegex() throws IOException {
    final String simpleXML = tmpDir.copyResourceFileName("simple.xml");

    FileInputStream fileInputStream = null;
    try {
      fileInputStream = new FileInputStream(new File(simpleXML));
      final XMLReader xmlReader = new XMLReader(fileInputStream, Pattern.compile("<[f]ood>"),
          Pattern.compile("<\\/[f]ood>"), 500, "UTF-8", 500);
      final Text readText = new Text();

      int i = 1;
      final ImmutableSet.Builder<String> recordsBuilder = ImmutableSet.builder();
      while (i != 0) {
        i = xmlReader.readXMLRecord(readText);
        recordsBuilder.add(readText.toString());
      }
      assertEquals(5, recordsBuilder.build().size());

    } finally {
      fileInputStream.close();
    }
  }

  @Test
  public void testReadAllRecordsRegexTwoRecordTypesBoth() throws IOException {
    final String doubleRecordXML = tmpDir.copyResourceFileName("doubleRecord.xml");

    FileInputStream fileInputStream1 = null;
    try {
      fileInputStream1 = new FileInputStream(new File(doubleRecordXML));
      final XMLReader xmlReader = new XMLReader(fileInputStream1, Pattern.compile("<[a-z][a-z][a-z][a-z][a-z]Food>"),
          Pattern.compile("<\\/[a-z][a-z][a-z][a-z][a-z]Food>"), 500, "UTF-8", 500);
      final Text readText = new Text();

      int i = 1;
      final ImmutableSet.Builder<String> recordsBuilder = ImmutableSet.builder();
      while (i != 0) {
        i = xmlReader.readXMLRecord(readText);
        recordsBuilder.add(readText.toString());
      }
      assertEquals(5, recordsBuilder.build().size());
    } finally {
      fileInputStream1.close();
    }
  }

  public void testReadAllRecordsRegexTwoRecordTypesOne() throws IOException {
    final String doubleRecordXML = tmpDir.copyResourceFileName("doubleRecord.xml");
    FileInputStream fileInputStream2 = null;
    try {
      fileInputStream2 = new FileInputStream(new File(doubleRecordXML));
      final XMLReader xmlReader2 = new XMLReader(fileInputStream2, Pattern.compile("<sweetFood>"),
          Pattern.compile("<\\/sweetFood>"), 500, "UTF-8", 500);
      final Text readText2 = new Text();

      int i2 = 1;
      final ImmutableSet.Builder<String> recordsBuilder2 = ImmutableSet.builder();
      while (i2 != 0) {
        i2 = xmlReader2.readXMLRecord(readText2);
        recordsBuilder2.add(readText2.toString());
      }
      assertEquals(3, recordsBuilder2.build().size());

    } finally {
      fileInputStream2.close();
    }
  }

  public void testReadAllRecordsRegexTwoRecordTypesO() throws IOException {
    final String doubleRecordXML = tmpDir.copyResourceFileName("doubleRecord.xml");
    FileInputStream fileInputStream3 = null;
    try {
      fileInputStream3 = new FileInputStream(new File(doubleRecordXML));
      final XMLReader xmlReader3 = new XMLReader(fileInputStream3, Pattern.compile("<saltyFood>"),
          Pattern.compile("<\\/saltyFood>"), 500, "UTF-8", 500);
      final Text readText3 = new Text();

      int i3 = 1;
      final ImmutableSet.Builder<String> recordsBuilder3 = ImmutableSet.builder();
      while (i3 != 0) {
        i3 = xmlReader3.readXMLRecord(readText3);
        recordsBuilder3.add(readText3.toString());
      }
      assertEquals(2, recordsBuilder3.build().size());
    } finally {
      fileInputStream3.close();
    }
  }
}
