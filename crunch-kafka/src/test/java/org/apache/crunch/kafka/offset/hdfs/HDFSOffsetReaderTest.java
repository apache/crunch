package org.apache.crunch.kafka.offset.hdfs;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.crunch.kafka.offset.OffsetReader;
import org.apache.crunch.kafka.offset.OffsetWriter;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class HDFSOffsetReaderTest {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  private Path basePath;
  private FileSystem fileSystem;
  private OffsetWriter writer;
  private OffsetReader reader;


  @Before
  public void setup() throws IOException {
    Configuration config = new Configuration();
    config.set(FileSystem.DEFAULT_FS, tempFolder.newFolder().getAbsolutePath());

    fileSystem = FileSystem.newInstance(config);
    basePath = new Path(tempFolder.newFolder().toString(), testName.getMethodName());

    writer = new HDFSOffsetWriter(config, basePath);

    reader = new HDFSOffsetReader(config, basePath);
  }

  @After
  public void cleanup() throws IOException {
    writer.close();
    reader.close();
    fileSystem.close();
  }

  @Test(expected = IllegalArgumentException.class)
  public void constructNullConfig() {
    new HDFSOffsetReader(null, new Path("/"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void constructNullPath() {
    new HDFSOffsetReader(new Configuration(), null);
  }

  @Test
  public void getStoredOffsetPersistenceTimesNoValues() throws IOException {
    List<Long> storedOffsetPersistenceTimes = reader.getStoredOffsetPersistenceTimes();
    assertThat(storedOffsetPersistenceTimes, is(Collections.<Long>emptyList()));
  }

  @Test
  public void getStoredOffsetPersistenceTimesMultipleValues() throws IOException {
    long current = 1464992662000L;
    List<Long> persistedTimes = new LinkedList<>();
    for (int i = 0; i < 10; i++) {
      persistedTimes.add(current + (i * 18000));
    }

    for (Long t : persistedTimes) {
      try {
        writer.write(t, Collections.<TopicPartition, Long>emptyMap());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    List<Long> storedTimes = reader.getStoredOffsetPersistenceTimes();

    assertThat(storedTimes, is(persistedTimes));
  }

  @Test
  public void readOffsetNoMatchForTime() throws IOException {
    Map<TopicPartition, Long> offsets = reader.readOffsets(12345L);
    assertThat(offsets, is(nullValue()));
  }

  @Test
  public void readOffsetLatestNone() throws IOException {
    assertThat(reader.readLatestOffsets(), is(Collections.<TopicPartition, Long>emptyMap()));
  }

  @Test
  public void readOffsetLatest() throws IOException {
    long current = 1464992662000L;
    List<Long> persistedTimes = new LinkedList<>();
    for (int i = 0; i < 10; i++) {
      persistedTimes.add(current + (i * 18000));
    }

    for (Long t : persistedTimes) {
      try {
        writer.write(t, Collections.<TopicPartition, Long>emptyMap());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    long expectedTime = persistedTimes.get(persistedTimes.size() - 1);

    Map<TopicPartition, Long> offsets = new HashMap<>();
    for (int i = 0; i < 9; i++) {
      for (int j = 0; j < 5; j++) {
        offsets.put(new TopicPartition("topic" + i, j), (long) j);
      }
    }

    writer.write(expectedTime, offsets);

    Map<TopicPartition, Long> retrievedOffsets = reader.readLatestOffsets();

    assertThat(retrievedOffsets, is(offsets));
  }


  @Test
  public void readOffsetForTime() throws IOException {
    long current = 1464992662000L;
    List<Long> persistedTimes = new LinkedList<>();
    for (int i = 0; i < 10; i++) {
      persistedTimes.add(current + (i * 18000));
    }
    for (Long t : persistedTimes) {
      try {
        writer.write(t, Collections.<TopicPartition, Long>emptyMap());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    long expectedTime = persistedTimes.get(2);

    Map<TopicPartition, Long> offsets = new HashMap<>();
    for (int i = 0; i < 9; i++) {
      for (int j = 0; j < 5; j++) {
        offsets.put(new TopicPartition("topic" + i, j), (long) j);
      }
    }

    writer.write(expectedTime, offsets);

    Map<TopicPartition, Long> retrievedOffsets = reader.readOffsets(expectedTime);

    assertThat(retrievedOffsets, is(offsets));
  }


  @Test
  public void skipReadingDirectory() throws IOException {
    long current = 1464992662000L;
    List<Long> persistedTimes = new LinkedList<>();
    for (int i = 0; i < 10; i++) {
      persistedTimes.add(current + (i * 18000));
    }

    for (Long t : persistedTimes) {
      try {
        writer.write(t, Collections.<TopicPartition, Long>emptyMap());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    fileSystem.mkdirs(new Path(basePath, "imadirectory"));

    List<Long> storedTimes = reader.getStoredOffsetPersistenceTimes();

    assertThat(storedTimes, is(persistedTimes));
  }

  @Test
  public void skipInvalidFile() throws IOException {
    long current = 1464992662000L;
    List<Long> persistedTimes = new LinkedList<>();
    for (int i = 0; i < 10; i++) {
      persistedTimes.add(current + (i * 18000));
    }

    for (Long t : persistedTimes) {
      try {
        writer.write(t, Collections.<TopicPartition, Long>emptyMap());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    fileSystem.createNewFile(new Path(basePath, "imabadfile.json"));
    fileSystem.createNewFile(new Path(basePath, "imabadfile.txt"));

    List<Long> storedTimes = reader.getStoredOffsetPersistenceTimes();

    assertThat(storedTimes, is(persistedTimes));
  }
}