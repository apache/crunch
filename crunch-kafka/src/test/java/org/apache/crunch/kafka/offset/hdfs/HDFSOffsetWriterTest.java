package org.apache.crunch.kafka.offset.hdfs;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class HDFSOffsetWriterTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  private Configuration config;

  private Path basePath;
  private FileSystem fileSystem;
  private HDFSOffsetWriter writer;


  @Before
  public void setup() throws IOException {
    config = new Configuration();
    config.set(FileSystem.DEFAULT_FS, tempFolder.newFolder().getAbsolutePath());

    fileSystem = FileSystem.newInstance(config);
    basePath = new Path(tempFolder.newFolder().toString(), testName.getMethodName());

    writer = new HDFSOffsetWriter(config, basePath);
  }

  @After
  public void cleanup() throws IOException {
    writer.close();
    fileSystem.close();
  }

  @Test(expected = IllegalArgumentException.class)
  public void constructNullConfig() {
    new HDFSOffsetWriter(null, new Path("/"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void constructNullPath() {
    new HDFSOffsetWriter(new Configuration(), null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void writeNullOffsets() throws IOException {
    writer.write(10L, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void writeNullInvalidAsOfTime() throws IOException {
    writer.write(-1L, Collections.<TopicPartition, Long>emptyMap());
  }

  @Test
  public void writeEmptyOffsets() throws IOException {
    long persistTime = System.currentTimeMillis();
    Map<TopicPartition, Long> offsets = Collections.emptyMap();

    writer.write(persistTime, offsets);

    Path expectedPath = HDFSOffsetWriter.getPersistedTimeStoragePath(basePath, persistTime);

    try (InputStream in = fileSystem.open(expectedPath)) {
      Offsets persistedOffsets = MAPPER.readValue(in, Offsets.class);
      assertThat(persistedOffsets.getAsOfTime(), is(persistTime));
      assertThat(persistedOffsets.getOffsets(), is(Collections.<Offsets.PartitionOffset>emptyList()));
    }
  }

  @Test
  public void writeOffsets() throws IOException {
    long persistTime = System.currentTimeMillis();
    Map<TopicPartition, Long> offsets = new HashMap<>();

    for (int i = 0; i < 9; i++) {
      for (int j = 0; j < 5; j++) {
        offsets.put(new TopicPartition("topic" + i, j), (long) j);
      }

    }

    writer.write(persistTime, offsets);

    Path expectedPath = HDFSOffsetWriter.getPersistedTimeStoragePath(basePath, persistTime);

    try (InputStream in = fileSystem.open(expectedPath)) {
      Offsets persistedOffsets = MAPPER.readValue(in, Offsets.class);
      assertThat(persistedOffsets.getAsOfTime(), is(persistTime));
      assertThat(persistedOffsets.getOffsets().size(), is(offsets.size()));

      Iterator<Offsets.PartitionOffset> partitionOffsets = persistedOffsets.getOffsets().iterator();
      for (int i = 0; i < 9; i++) {
        for (int j = 0; j < 5; j++) {
          assertTrue(partitionOffsets.hasNext());
          Offsets.PartitionOffset partitionOffset = partitionOffsets.next();
          assertThat(partitionOffset.getPartition(), is(j));
          assertThat(partitionOffset.getOffset(), is((long) j));
          assertThat(partitionOffset.getTopic(), is("topic" + i));
        }
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void getPersistedStoragePathNullBase() {
    HDFSOffsetWriter.getPersistedTimeStoragePath(null, 10L);
  }

  @Test
  public void getPersistedStoragePath() {
    //Timestamp of 02 Jun 2016 20:12:17 GMT
    //2016-06-02T20:12:17Z
    long timestamp = 1464898337000L;

    String expectedFileName = HDFSOffsetWriter.FILE_FORMATTER.print(timestamp)
        + HDFSOffsetWriter.FILE_FORMAT_EXTENSION;
    Path filePath = HDFSOffsetWriter.getPersistedTimeStoragePath(basePath, timestamp);

    assertThat(filePath, is(new Path(basePath, expectedFileName)));
  }

  @Test
  public void timeToFileName() {
    //Timestamp of 02 Jun 2016 20:12:17 GMT
    //2016-06-02T20:12:17Z
    long timestamp = 1464898337000L;

    String expectedFileName = "2016-06-02T20-12-17+0000" + HDFSOffsetWriter.FILE_FORMAT_EXTENSION;

    assertThat(HDFSOffsetWriter.persistenceTimeToFileName(timestamp), is(expectedFileName));
  }

  @Test
  public void fileNameToTime() {
    //Timestamp of 02 Jun 2016 20:12:17 GMT
    //2016-06-02T20:12:17Z
    long timestamp = 1464898337000L;

    String expectedFileName = "2016-06-02T20-12-17+0000" + HDFSOffsetWriter.FILE_FORMAT_EXTENSION;

    assertThat(HDFSOffsetWriter.fileNameToPersistenceTime(expectedFileName), is(timestamp));
  }

  @Test(expected = IllegalArgumentException.class)
  public void fileNameToTimeNullFileName() {
    HDFSOffsetWriter.fileNameToPersistenceTime(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void fileNameToTimeEmptyFileName() {
    HDFSOffsetWriter.fileNameToPersistenceTime("");
  }

  @Test(expected = IllegalArgumentException.class)
  public void fileNameToTimeInvalidFileName() {
    HDFSOffsetWriter.fileNameToPersistenceTime("2016-06-02T20:12:17.000Z.json");
  }
}
