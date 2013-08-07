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
package org.apache.crunch.types;

import java.util.Iterator;
import java.util.List;

import org.apache.crunch.GroupingOptions;
import org.apache.crunch.MapFn;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.Pair;
import org.apache.crunch.io.ReadableSourceTarget;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.google.common.collect.Iterables;

/**
 * The {@code PType} instance for {@link PGroupedTable} instances. Its settings
 * are derived from the {@code PTableType} that was grouped to create the
 * {@code PGroupedTable} instance.
 * 
 */
public abstract class PGroupedTableType<K, V> implements PType<Pair<K, Iterable<V>>> {

  protected static class PTypeIterable<V> implements Iterable<V> {
    private final Iterable<Object> iterable;
    private final HoldLastIterator<V> holdLastIter;

    public PTypeIterable(MapFn<Object, V> mapFn, Iterable<Object> iterable) {
      this.iterable = iterable;
      this.holdLastIter = new HoldLastIterator<V>(mapFn);
    }

    public Iterator<V> iterator() {
      return holdLastIter.reset(iterable.iterator());
    }
    
    @Override
    public String toString() {
      return holdLastIter.toString();
    }
  }

  protected static class HoldLastIterator<V> implements Iterator<V> {

    private Iterator<Object> iter;
    private V lastReturned = null;
    private final MapFn<Object, V> mapFn;
    
    public HoldLastIterator(MapFn<Object, V> mapFn) {
      this.mapFn = mapFn;
    }
    
    public HoldLastIterator<V> reset(Iterator<Object> iter) {
      this.iter = iter;
      return this;
    }
    
    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public V next() {
      lastReturned = mapFn.map(iter.next());
      return lastReturned;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder().append('[');
      if (lastReturned != null) {
        sb.append(lastReturned).append(", ...]");
      } else if (iter != null) {
        sb.append("...]");
      }
      return sb.toString();
    }
  }
  
  public static class PairIterableMapFn<K, V> extends MapFn<Pair<Object, Iterable<Object>>, Pair<K, Iterable<V>>> {
    private final MapFn<Object, K> keys;
    private final MapFn<Object, V> values;

    public PairIterableMapFn(MapFn<Object, K> keys, MapFn<Object, V> values) {
      this.keys = keys;
      this.values = values;
    }

    @Override
    public void configure(Configuration conf) {
      keys.configure(conf);
      values.configure(conf);
    }
    
    public void setContext(TaskInputOutputContext<?, ?, ?, ?> context) {
      keys.setContext(context);
      values.setContext(context);
    }
    
    @Override
    public void initialize() {
      keys.initialize();
      values.initialize();
    }

    @Override
    public Pair<K, Iterable<V>> map(Pair<Object, Iterable<Object>> input) {
      return Pair.<K, Iterable<V>> of(keys.map(input.first()), new PTypeIterable(values, input.second()));
    }
  }

  protected final PTableType<K, V> tableType;

  public PGroupedTableType(PTableType<K, V> tableType) {
    this.tableType = tableType;
  }

  public PTableType<K, V> getTableType() {
    return tableType;
  }

  @Override
  public PTypeFamily getFamily() {
    return tableType.getFamily();
  }

  @Override
  public List<PType> getSubTypes() {
    return tableType.getSubTypes();
  }

  @Override
  public Converter getConverter() {
    return tableType.getConverter();
  }

  public abstract Converter getGroupingConverter();

  public abstract void configureShuffle(Job job, GroupingOptions options);

  @Override
  public ReadableSourceTarget<Pair<K, Iterable<V>>> getDefaultFileSource(Path path) {
    throw new UnsupportedOperationException("Grouped tables cannot be written out directly");
  }
}
