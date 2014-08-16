package org.apache.crunch.io.xml;

import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.regex.Pattern;

import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;

public class XMLFileSourceIT {
  @Rule
  public transient TemporaryPath tmpDir = TemporaryPaths.create();

  private final String[] expectedFileContents = {
      "<food>\n\t\t<name>Belgian Waffles</name>\n\t\t<price>$5.95</price>\n\t\t<description>Two of our famous Belgian Waffles with plenty of real maple syrup</description>\n\t\t<calories>650</calories>\n\t</food>",
      "<food>\n\t\t<name>Strawberry Belgian Waffles</name>\n\t\t<price>$7.95</price>\n\t\t<description>Light Belgian waffles covered with strawberries and whipped cream</description>\n\t\t<calories>900</calories>\n\t</food>",
      "<food>\n\t\t<name>Berry-Berry Belgian Waffles</name>\n\t\t<price>$8.95</price>\n\t\t<description>Light Belgian waffles covered with an assortment of fresh berries and whipped cream</description>\n\t\t<calories>900</calories>\n\t</food>",
      "<food>\n\t\t<name>French Toast</name>\n\t\t<price>$4.50</price>\n\t\t<description>Thick slices made from our homemade sourdough bread</description>\n\t\t<calories>600</calories>\n\t</food>",
      "<food>\n\t\t<name>Homestyle Breakfast</name>\n\t\t<price>$6.95</price>\n\t\t<description>Two eggs, bacon or sausage, toast, and our ever-popular hash browns</description>\n\t\t<calories>950</calories>\n\t</food>" };

  @Test
  public void testVanillaXML() throws Exception {

    final String vanillaXMLFile = tmpDir.copyResourceFileName("simple.xml");
    final Pipeline pipeline = new MRPipeline(XMLFileSourceIT.class, tmpDir.getDefaultConfiguration());
    final PCollection<String> xmlRecords = pipeline.read(new XMLFileSource(new Path(vanillaXMLFile), Pattern
        .compile("<food>"), Pattern.compile("</food>")));

    final Collection<String> csvRecordsList = xmlRecords.asCollection().getValue();

    for (int i = 0; i < expectedFileContents.length; i++) {
      assertTrue(csvRecordsList.contains(expectedFileContents[i]));
    }
  }

  @Test
  public void testVanillaXMLWithAdditionalActions() throws Exception {

    final String vanillaXMLFile = tmpDir.copyResourceFileName("simple.xml");
    final Pipeline pipeline = new MRPipeline(XMLFileSourceIT.class, tmpDir.getDefaultConfiguration());
    final PCollection<String> xmlRecords = pipeline.read(new XMLFileSource(new Path(vanillaXMLFile), Pattern
        .compile("<food>"), Pattern.compile("</food>")));

    final PTable<String, Long> countTable = xmlRecords.count();
    final PCollection<String> xmlRecords2 = countTable.keys();
    final Collection<String> csvRecordsList2 = xmlRecords2.asCollection().getValue();

    for (int i = 0; i < expectedFileContents.length; i++) {
      assertTrue(csvRecordsList2.contains(expectedFileContents[i]));
    }
  }

  private final String[] expectedChineseWords = {
      "<word>\n\t\t<characters>认识</characters>\n\t\t<pinyin>rèn shi</pinyin>\n\t\t<english>to know / recognize</english>\n\t</word>",
      "<word>\n\t\t<characters>男生</characters>\n\t\t<pinyin>nán shēng</pinyin>\n\t\t<english>male students</english>\n\t</word>",
      "<word>\n\t\t<characters>女生</characters>\n\t\t<pinyin>nǚ shēng</pinyin>\n\t\t<english>female students</english>\n\t</word>",
      "<word>\n\t\t<characters>年级</characters>\n\t\t<pinyin>nián jí</pinyin>\n\t\t<english>grade (school)</english>\n\t</word>",
      "<word>\n\t\t<characters>班</characters>\n\t\t<pinyin>bān</pinyin>\n\t\t<english>class</english>\n\t</word>",
      "<word>\n\t\t<characters>新</characters>\n\t\t<pinyin>xīn</pinyin>\n\t\t<english>new</english>\n\t</word>",
      "<word>\n\t\t<characters>介绍</characters>\n\t\t<pinyin>jiè shào</pinyin>\n\t\t<english>introduce</english>\n\t</word>",
      "<word>\n\t\t<characters>欢迎</characters>\n\t\t<pinyin>huān yíng</pinyin>\n\t\t<english>welcome</english>\n\t</word>",
      "<word>\n\t\t<characters>新来</characters>\n\t\t<pinyin>xīn lái</pinyin>\n\t\t<english>newcomer</english>\n\t</word>",
      "<word>\n\t\t<characters>借</characters>\n\t\t<pinyin>jiè</pinyin>\n\t\t<english>borrow / lend / make use of</english>\n\t</word>",
      "<word>\n\t\t<characters>能</characters>\n\t\t<pinyin>néng</pinyin>\n\t\t<english>ability / skill</english>\n\t</word>" };

  private final String[] expectedChinesePhrases = {
      "<phrase>\n\t\t<characters>你早</characters>\n\t\t<pinyin>nǐ zǎo</pinyin>\n\t\t<english>good morning to you</english>\n\t</phrase>",
      "<phrase>\n\t\t<characters>我来介绍</characters>\n\t\t<pinyin>wǒ lái jiè shào</pinyin>\n\t\t<english>I will introduce you</english>\n\t</phrase>",
      "<phrase>\n\t\t<characters>认识你们很高兴</characters>\n\t\t<pinyin>rèn shi nǐ men hěn gāo xìng</pinyin>\n\t\t<english>i am pleased to meet you</english>\n\t</phrase>" };

  @Test
  public void testChineseWordsWithAdditionalActions() throws Exception {

    final String vanillaXMLFile = tmpDir.copyResourceFileName("chineseVocab.xml");
    final Pipeline pipeline = new MRPipeline(XMLFileSourceIT.class, tmpDir.getDefaultConfiguration());
    final PCollection<String> xmlRecords = pipeline.read(new XMLFileSource(new Path(vanillaXMLFile), Pattern
        .compile("<word>"), Pattern.compile("</word>")));

    final PTable<String, Long> countTable = xmlRecords.count();
    final PCollection<String> xmlRecords2 = countTable.keys();
    final Collection<String> csvRecordsList2 = xmlRecords2.asCollection().getValue();

    for (int i = 0; i < expectedChineseWords.length; i++) {
      assertTrue(csvRecordsList2.contains(expectedChineseWords[i]));
    }
  }

  @Test
  public void testChinesePhrasesWithAdditionalActions() throws Exception {

    final String vanillaXMLFile = tmpDir.copyResourceFileName("chineseVocab.xml");
    final Pipeline pipeline = new MRPipeline(XMLFileSourceIT.class, tmpDir.getDefaultConfiguration());
    final PCollection<String> xmlRecords = pipeline.read(new XMLFileSource(new Path(vanillaXMLFile), Pattern
        .compile("<phrase>"), Pattern.compile("</phrase>")));

    final PTable<String, Long> countTable = xmlRecords.count();
    final PCollection<String> xmlRecords2 = countTable.keys();
    final Collection<String> csvRecordsList2 = xmlRecords2.asCollection().getValue();

    for (int i = 0; i < expectedChinesePhrases.length; i++) {
      assertTrue(csvRecordsList2.contains(expectedChinesePhrases[i]));
    }
  }

  @Test
  public void testChineseAllWithAdditionalActions() throws Exception {

    final String vanillaXMLFile = tmpDir.copyResourceFileName("chineseVocab.xml");
    final Pipeline pipeline = new MRPipeline(XMLFileSourceIT.class, tmpDir.getDefaultConfiguration());
    final PCollection<String> xmlRecords = pipeline.read(new XMLFileSource(new Path(vanillaXMLFile), Pattern
        .compile("<phrase>|<word>"), Pattern.compile("</phrase>|</word>")));

    final PTable<String, Long> countTable = xmlRecords.count();
    final PCollection<String> xmlRecords2 = countTable.keys();
    final Collection<String> csvRecordsList2 = xmlRecords2.asCollection().getValue();

    for (int i = 0; i < expectedChinesePhrases.length; i++) {
      assertTrue(csvRecordsList2.contains(expectedChineseWords[i]));
    }
    for (int i = 0; i < expectedChinesePhrases.length; i++) {
      assertTrue(csvRecordsList2.contains(expectedChineseWords[i]));
    }
  }
}
