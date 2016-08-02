/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.crunch.kafka.offset.hdfs;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.crunch.kafka.offset.AbstractOffsetReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Reader implementation that reads offset information from HDFS.
 */
public class HDFSOffsetReader extends AbstractOffsetReader {

  private static final Logger LOG = LoggerFactory.getLogger(HDFSOffsetReader.class);

  private final Configuration config;
  private final Path baseOffsetStoragePath;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Creates a reader instance for interacting with the storage specified by the {@code config} and with
   * the base storage path of {@code baseStoragePath}.
   *
   * @param config                the config for interacting with the underlying data store.
   * @param baseOffsetStoragePath the base storage path for offset information.  If the path does not exist it will
   *                              be created.
   * @throws IllegalArgumentException if either argument is {@code null}.
   */
  public HDFSOffsetReader(Configuration config, Path baseOffsetStoragePath) {
    if (config == null) {
      throw new IllegalArgumentException("The 'config' cannot be 'null'.");
    }
    if (baseOffsetStoragePath == null) {
      throw new IllegalArgumentException("The 'baseOffsetStoragePath' cannot be 'null'.");
    }
    this.config = config;
    this.baseOffsetStoragePath = baseOffsetStoragePath;
  }

  @Override
  public Map<TopicPartition, Long> readLatestOffsets() throws IOException {
    List<Long> storedOffsetPersistenceTimes = getStoredOffsetPersistenceTimes(true);
    if (storedOffsetPersistenceTimes.isEmpty()) {
      return Collections.emptyMap();
    }

    long persistedTime = storedOffsetPersistenceTimes.get(0);

    Map<TopicPartition, Long> offsets = readOffsets(persistedTime);

    return offsets == null ? Collections.<TopicPartition, Long>emptyMap() : offsets;
  }

  @Override
  public Map<TopicPartition, Long> readOffsets(long persistedOffsetTime) throws IOException {
    Path offsetFilePath = HDFSOffsetWriter.getPersistedTimeStoragePath(baseOffsetStoragePath, persistedOffsetTime);

    FileSystem fs = getFileSystem();
    if (fs.isFile(offsetFilePath)) {
      InputStream inputStream = fs.open(offsetFilePath);
      try  {
        Offsets offsets = MAPPER.readValue(inputStream, Offsets.class);
        Map<TopicPartition, Long> partitionsMap = new HashMap<>();
        for(Offsets.PartitionOffset partitionOffset: offsets.getOffsets()){
          partitionsMap.put(new TopicPartition(partitionOffset.getTopic(), partitionOffset.getPartition()),
              partitionOffset.getOffset());
        }
        return partitionsMap;
      }finally{
        inputStream.close();
      }
    }

    LOG.error("Offset file at {} is not a file or does not exist.", offsetFilePath);
    return null;
  }

  @Override
  public List<Long> getStoredOffsetPersistenceTimes() throws IOException {
    return getStoredOffsetPersistenceTimes(false);
  }

  private List<Long> getStoredOffsetPersistenceTimes(boolean newestFirst) throws IOException {
    List<Long> persistedTimes = new LinkedList<>();
    FileSystem fs = getFileSystem();
    try {
      FileStatus[] fileStatuses = fs.listStatus(baseOffsetStoragePath);
      for (FileStatus status : fileStatuses) {
        if (status.isFile()) {
          String fileName = status.getPath().getName();
          try {
            persistedTimes.add(HDFSOffsetWriter.fileNameToPersistenceTime(fileName));
          } catch (IllegalArgumentException iae) {
            LOG.info("Skipping file {} due to filename not being of the correct format.", status.getPath(),
                iae);
          }
        } else {
          LOG.info("Skippping {} because it is not a file.", status.getPath());
        }
      }
    } catch (FileNotFoundException fnfe) {
      LOG.error("Unable to retrieve prior offsets.", fnfe);
    }

    //natural order should put oldest (smallest long) first. This will put newest first.
    if (newestFirst) {
      Collections.sort(persistedTimes, Collections.reverseOrder());
    } else {
      Collections.sort(persistedTimes);
    }
    return Collections.unmodifiableList(persistedTimes);
  }

  @Override
  public void close() throws IOException {

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
}
