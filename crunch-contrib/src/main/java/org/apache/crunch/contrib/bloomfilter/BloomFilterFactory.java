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
package org.apache.crunch.contrib.bloomfilter;

import java.io.IOException;
import java.util.Map;

import org.apache.crunch.Aggregator;
import org.apache.crunch.PCollection;
import org.apache.crunch.PObject;
import org.apache.crunch.PTable;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.materialize.pobject.FirstElementPObject;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.BloomFilter;

import com.google.common.collect.ImmutableList;

/**
 * Factory Class for creating BloomFilters. The APIs require a
 * {@link BloomFilterFn} which is responsible for generating keys of the filter.
 */
public class BloomFilterFactory {
  /**
   * The method will take an input path and generates BloomFilters for all text
   * files in that path. The method return back a {@link PObject} containing a
   * {@link Map} having file names as keys and filters as values
   */
  public static PObject<Map<String, BloomFilter>> createFilter(Path inputPath, BloomFilterFn<String> filterFn)
      throws IOException {
    MRPipeline pipeline = new MRPipeline(BloomFilterFactory.class);
    FileStatus[] listStatus = FileSystem.get(pipeline.getConfiguration()).listStatus(inputPath);
    PTable<String, BloomFilter> filterTable = null;
    for (FileStatus fileStatus : listStatus) {
      Path path = fileStatus.getPath();
      PCollection<String> readTextFile = pipeline.readTextFile(path.toString());
      pipeline.getConfiguration().set(BloomFilterFn.CRUNCH_FILTER_NAME, path.getName());
      PTable<String, BloomFilter> currentTable = createFilterTable(readTextFile, filterFn);
      if (filterTable != null) {
        filterTable = filterTable.union(currentTable);
      } else {
        filterTable = currentTable;
      }
    }
    return filterTable.asMap();
  }

  public static <T> PObject<BloomFilter> createFilter(PCollection<T> collection, BloomFilterFn<T> filterFn) {
    collection.getPipeline().getConfiguration().set(BloomFilterFn.CRUNCH_FILTER_NAME, collection.getName());
    return new FirstElementPObject<BloomFilter>(createFilterTable(collection, filterFn).values());
  }

  private static <T> PTable<String, BloomFilter> createFilterTable(PCollection<T> collection, BloomFilterFn<T> filterFn) {
    PTypeFamily tf = collection.getTypeFamily();
    PTable<String, BloomFilter> table = collection.parallelDo(filterFn,
        tf.tableOf(tf.strings(), Writables.writables(BloomFilter.class)));
    return table.groupByKey(1).combineValues(new BloomFilterAggregator());
  }


  @SuppressWarnings("serial")
  private static class BloomFilterAggregator implements Aggregator<BloomFilter> {
    private transient BloomFilter bloomFilter = null;
    private transient int filterSize;

    @Override
    public void update(BloomFilter value) {
      bloomFilter.or(value);
    }

    @Override
    public Iterable<BloomFilter> results() {
      return ImmutableList.of(bloomFilter);
    }

    @Override
    public void initialize(Configuration configuration) {
      filterSize = BloomFilterFn.getBloomFilterSize(configuration);
    }

    @Override
    public void reset() {
      bloomFilter = BloomFilterFn.initializeFilter(filterSize);

    }

  }

}
