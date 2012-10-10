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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.crunch.test.CrunchTestSupport;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.junit.Test;

public class BloomFiltersIT extends CrunchTestSupport implements Serializable {

  @Test
  public void testFilterCreation() throws IOException {
    String inputPath = tempDir.copyResourceFileName("shakes.txt");
    BloomFilterFn<String> filterFn = new BloomFilterFn<String>() {
      @Override
      public Collection<Key> generateKeys(String input) {
        List<String> parts = Arrays.asList(StringUtils.split(input, " "));
        Collection<Key> keys = new HashSet<Key>();
        for (String stringpart : parts) {
          keys.add(new Key(stringpart.getBytes()));
        }
        return keys;
      }
    };
    Map<String, BloomFilter> filterValues = BloomFilterFactory.createFilter(new Path(inputPath), filterFn).getValue();
    assertEquals(1, filterValues.size());
    BloomFilter filter = filterValues.get("shakes.txt");
    assertTrue(filter.membershipTest(new Key("Mcbeth".getBytes())));
    assertTrue(filter.membershipTest(new Key("apples".getBytes())));
  }

}
