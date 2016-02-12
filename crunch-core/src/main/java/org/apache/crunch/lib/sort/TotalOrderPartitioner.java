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
package org.apache.crunch.lib.sort;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.crunch.io.CompositePathIterable;
import org.apache.crunch.io.avro.AvroFileReaderFactory;
import org.apache.crunch.io.seq.SeqFileReaderFactory;
import org.apache.crunch.types.writable.WritableDeepCopier;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * A partition-aware {@code Partitioner} instance that can work with either Avro or Writable-formatted
 * keys.
 */
public class TotalOrderPartitioner<K, V> extends Partitioner<K, V> implements Configurable {

  public static final String DEFAULT_PATH = "_partition.lst";
  public static final String PARTITIONER_PATH = 
    "crunch.totalorderpartitioner.path";
  
  private Configuration conf;
  private Node<K> partitions;
  
  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    try {
      this.conf = conf;
      String parts = getPartitionFile(conf);
      final Path partFile = new Path(parts);
      final FileSystem fs = (DEFAULT_PATH.equals(parts))
        ? FileSystem.getLocal(conf)     // assume in DistributedCache
        : partFile.getFileSystem(conf);

      Job job = new Job(conf);
      Class<K> keyClass = (Class<K>)job.getMapOutputKeyClass();
      RawComparator<K> comparator =
          (RawComparator<K>) job.getSortComparator();
      K[] splitPoints = readPartitions(fs, partFile, keyClass, conf, comparator);
      int numReduceTasks = job.getNumReduceTasks();
      if (splitPoints.length != numReduceTasks - 1) {
        throw new IOException("Wrong number of partitions in keyset");
      }
      partitions = new BinarySearchNode(splitPoints, comparator);
    } catch (IOException e) {
      throw new IllegalArgumentException("Can't read partitions file", e);
    }
  }

  @Override
  public int getPartition(K key, V value, int modulo) {
    return partitions.findPartition(key);
  }

  public static void setPartitionFile(Configuration conf, Path p) {
    conf.set(PARTITIONER_PATH, p.toString());
  }

  public static String getPartitionFile(Configuration conf) {
    return conf.get(PARTITIONER_PATH, DEFAULT_PATH);
  }
  
  @SuppressWarnings("unchecked") // map output key class
  private K[] readPartitions(FileSystem fs, Path p, Class<K> keyClass,
      Configuration conf, final RawComparator<K> comparator) throws IOException {
    ArrayList<K> parts = new ArrayList<K>();
    String schema = conf.get("crunch.schema");
    if (schema != null) {
      Schema s = (new Schema.Parser()).parse(schema);
      AvroFileReaderFactory<K> a = new AvroFileReaderFactory<K>(s);
      Iterator<K> iter = CompositePathIterable.create(fs, p, a).iterator();
      while (iter.hasNext()) {
        parts.add((K) new AvroKey<K>(iter.next()));
      }
    } else {
      WritableDeepCopier wdc = new WritableDeepCopier(keyClass);
      SeqFileReaderFactory<K> s = new SeqFileReaderFactory<K>(keyClass);
      Iterator<K> iter = CompositePathIterable.create(fs, p, s).iterator();
      while (iter.hasNext()) {
        parts.add((K) wdc.deepCopy((Writable) iter.next()));
      }
    }
    Collections.sort(parts, comparator);
    return parts.toArray((K[])Array.newInstance(keyClass, parts.size()));
  }
  
  /**
   * Interface to the partitioner to locate a key in the partition keyset.
   */
  public interface Node<T> {
    /**
     * Locate partition in keyset K, st [Ki..Ki+1) defines a partition,
     * with implicit K0 = -inf, Kn = +inf, and |K| = #partitions - 1.
     */
    int findPartition(T key);
  }

  public static class BinarySearchNode<K> implements Node<K> {
    private final K[] splitPoints;
    private final RawComparator<K> comparator;
    public BinarySearchNode(K[] splitPoints, RawComparator<K> comparator) {
      this.splitPoints = splitPoints;
      this.comparator = comparator;
    }
    public int findPartition(K key) {
      final int pos = Arrays.binarySearch(splitPoints, key, comparator) + 1;
      return (pos < 0) ? -pos : pos;
    }
  }
}
