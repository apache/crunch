package org.apache.crunch.kafka.offset.hdfs;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.crunch.kafka.offset.AbstractOffsetWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Offset writer implementation that stores the offsets in HDFS.
 */
public class HDFSOffsetWriter extends AbstractOffsetWriter {

  private static final Logger LOG = LoggerFactory.getLogger(HDFSOffsetWriter.class);

  /**
   * Custom formatter for translating the times into valid file names.
   */
  public static final String PERSIST_TIME_FORMAT = "yyyy-MM-dd'T'HH-mm-ssZ";

  /**
   * Formatter to use when creating the file names in a URI compliant format.
   */
  public static final DateTimeFormatter FILE_FORMATTER = DateTimeFormat.forPattern(PERSIST_TIME_FORMAT).withZoneUTC();

  /**
   * File extension for storing the offsets.
   */
  public static final String FILE_FORMAT_EXTENSION = ".json";

  /**
   * Configuration for the underlying storage.
   */
  private final Configuration config;

  /**
   * Mapper for converting data into JSON
   */
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Base storage path for offset data
   */
  private final Path baseStoragePath;

  /**
   * Creates a writer instance for interacting with the storage specified by the {@code config} and with
   * the base storage path of {@code baseStoragePath}.
   *
   * @param config          the config for interacting with the underlying data store.
   * @param baseStoragePath the base storage path for offset information.
   * @throws IllegalArgumentException if either argument is {@code null}.
   */
  public HDFSOffsetWriter(Configuration config, Path baseStoragePath) {
    if (config == null) {
      throw new IllegalArgumentException("The 'config' cannot be 'null'.");
    }
    if (baseStoragePath == null) {
      throw new IllegalArgumentException("The 'baseStoragePath' cannot be 'null'.");
    }
    this.config = config;
    this.baseStoragePath = baseStoragePath;
  }

  @Override
  public void write(long asOfTime, Map<TopicPartition, Long> offsets) throws IOException {
    if (offsets == null) {
      throw new IllegalArgumentException("The 'offsets' cannot be 'null'.");
    }
    if (asOfTime < 0) {
      throw new IllegalArgumentException("The 'asOfTime' cannot be less than 0.");
    }
    List<Offsets.PartitionOffset> partitionOffsets = new LinkedList<>();
    for(Map.Entry<TopicPartition, Long> entry: offsets.entrySet()){
      partitionOffsets.add(Offsets.PartitionOffset.Builder.newBuilder().setOffset(entry.getValue())
          .setTopic(entry.getKey().topic())
          .setPartition(entry.getKey().partition()).build());
    }

    Offsets storageOffsets = Offsets.Builder.newBuilder().setOffsets(partitionOffsets)
        .setAsOfTime(asOfTime).build();

    FileSystem fs = getFileSystem();
    Path offsetPath = getPersistedTimeStoragePath(baseStoragePath, asOfTime);
    LOG.debug("Writing offsets to {} with as of time {}", offsetPath, asOfTime);
    try (FSDataOutputStream fsDataOutputStream = fs.create(getPersistedTimeStoragePath(baseStoragePath, asOfTime), true)) {
      MAPPER.writeValue(fsDataOutputStream, storageOffsets);
      fsDataOutputStream.flush();
    }
    LOG.debug("Completed writing offsets to {}", offsetPath);
  }

  @Override
  public void close() throws IOException {
    //no-op
  }

  /**
   * Returns the {@link FileSystem} instance for writing data.  Callers are not responsible for closing the instance.
   *
   * @return the {@link FileSystem} instance for writing data.
   * @throws IOException error retrieving underlying file system.
   */
  protected FileSystem getFileSystem() throws IOException {
    return FileSystem.get(config);
  }

  /**
   * Creates a {@link Path} for storing the offsets for a specified {@code persistedTime}.
   *
   * @param baseStoragePath The base path the offsets will be stored at.
   * @param persistedTime   the time of the data being persisted.
   * @return The path to where the offset information should be stored.
   * @throws IllegalArgumentException if the {@code baseStoragePath} is {@code null}.
   */
  public static Path getPersistedTimeStoragePath(Path baseStoragePath, long persistedTime) {
    if (baseStoragePath == null) {
      throw new IllegalArgumentException("The 'baseStoragePath' cannot be 'null'.");
    }
    return new Path(baseStoragePath, persistenceTimeToFileName(persistedTime));
  }

  /**
   * Converts a {@code fileName} into the time the offsets were persisted.
   *
   * @param fileName the file name to parse.
   * @return the time in milliseconds since epoch that the offsets were stored.
   * @throws IllegalArgumentException if the {@code fileName} is not of the correct format or is {@code null} or
   *                                  empty.
   */
  public static long fileNameToPersistenceTime(String fileName) {
    if (StringUtils.isBlank(fileName)) {
      throw new IllegalArgumentException("the 'fileName' cannot be 'null' or empty");
    }
    String formattedTimeString = StringUtils.strip(fileName, FILE_FORMAT_EXTENSION);
    DateTime persistedTime = FILE_FORMATTER.parseDateTime(formattedTimeString);
    return persistedTime.getMillis();
  }

  /**
   * Converts a {@code persistedTime} into a file name for persisting the offsets.
   *
   * @param persistedTime the persisted time to use to generate the file name.
   * @return the file name to use when persisting the data.
   */
  public static String persistenceTimeToFileName(long persistedTime) {
    DateTime dateTime = new DateTime(persistedTime, DateTimeZone.UTC);
    String formattedTime = FILE_FORMATTER.print(dateTime);
    return formattedTime + FILE_FORMAT_EXTENSION;
  }
}