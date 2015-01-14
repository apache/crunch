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
package org.apache.crunch.io.hbase;

import com.google.common.collect.ImmutableList;
import org.apache.crunch.io.FileReaderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;

import java.io.IOException;
import java.util.Iterator;

public class HFileReaderFactory implements FileReaderFactory<KeyValue> {

  public static final String HFILE_SCANNER_CACHE_BLOCKS = "crunch.hfile.scanner.cache.blocks";
  public static final String HFILE_SCANNER_PREAD = "crunch.hfile.scanner.pread";

  @Override
  public Iterator<KeyValue> read(FileSystem fs, Path path) {
    Configuration conf = fs.getConf();
    CacheConfig cacheConfig = new CacheConfig(conf);
    try {
      HFile.Reader hfr = HFile.createReader(fs, path, cacheConfig, conf);
      HFileScanner scanner = hfr.getScanner(
          conf.getBoolean(HFILE_SCANNER_CACHE_BLOCKS, false),
          conf.getBoolean(HFILE_SCANNER_PREAD, false));
      scanner.seekTo();
      return new HFileIterator(scanner);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static class HFileIterator implements Iterator<KeyValue> {

    private final HFileScanner scanner;
    private KeyValue curr;

    public HFileIterator(HFileScanner scanner) {
      this.scanner = scanner;
      this.curr = KeyValue.cloneAndAddTags(scanner.getKeyValue(), ImmutableList.<Tag>of());
    }

    @Override
    public boolean hasNext() {
      return curr != null;
    }

    @Override
    public KeyValue next() {
      KeyValue ret = curr;
      try {
        if (scanner.next()) {
          curr = KeyValue.cloneAndAddTags(scanner.getKeyValue(), ImmutableList.<Tag>of());
        } else {
          curr = null;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return ret;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("HFileIterator is read-only");
    }
  }
}
