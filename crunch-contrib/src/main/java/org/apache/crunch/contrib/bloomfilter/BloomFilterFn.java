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

import java.util.Collection;

import org.apache.commons.collections.CollectionUtils;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

/**
 * The class is responsible for generating keys that are used in a BloomFilter
 */
@SuppressWarnings("serial")
public abstract class BloomFilterFn<S> extends DoFn<S, Pair<String, BloomFilter>> {
  public static final String CRUNCH_FILTER_SIZE = "crunch.filter.size";
  public static final String CRUNCH_FILTER_NAME = "crunch.filter.name";
  private transient BloomFilter bloomFilter = null;

  @Override
  public void initialize() {
    super.initialize();
    bloomFilter = initializeFilter(getBloomFilterSize(getConfiguration()));
  }

  @Override
  public void process(S input, Emitter<Pair<String, BloomFilter>> emitter) {
    Collection<Key> keys = generateKeys(input);
    if (CollectionUtils.isNotEmpty(keys)) {
      bloomFilter.add(keys);
    }
  }

  public abstract Collection<Key> generateKeys(S input);

  @Override
  public void cleanup(Emitter<Pair<String, BloomFilter>> emitter) {
    String filterName = getConfiguration().get(CRUNCH_FILTER_NAME);
    emitter.emit(Pair.of(filterName, bloomFilter));
  }

  static BloomFilter initializeFilter(int size) {
    return new BloomFilter(size, 5, Hash.MURMUR_HASH);
  }

  static int getBloomFilterSize(Configuration configuration) {
    return configuration.getInt(CRUNCH_FILTER_SIZE, 1024);
  }
}