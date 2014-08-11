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
package org.apache.crunch.impl.mem.collect;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.google.common.collect.ImmutableList;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.SingleUseIterable;
import org.apache.crunch.types.PType;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * In-memory versions of common MapReduce patterns for aggregating key-value data.
 */
abstract class Shuffler<K, V> implements Iterable<Pair<K, Iterable<V>>> {

  public abstract void add(Pair<K, V> record);
  
  private static <K, V> Map<K, V> getMapForKeyType(PType<?> ptype) {
    if (ptype != null && Comparable.class.isAssignableFrom(ptype.getTypeClass())) {
      return new TreeMap<K, V>();
    } else {
      return Maps.newHashMap();
    }
  }
  
  public static <S, T> Shuffler<S, T> create(PType<S> keyType, GroupingOptions options,
      Pipeline pipeline) {
    Map<Object, Collection<T>> map = getMapForKeyType(keyType);
    
    if (options != null) {
      Job job;
      try {
        job = new Job(pipeline.getConfiguration());
      } catch (IOException e) {
        throw new IllegalStateException("Could not create Job instance", e);
      }
      options.configure(job);
      if (Pair.class.equals(keyType.getTypeClass()) && options.getGroupingComparatorClass() != null) {
        PType<?> pairKey = keyType.getSubTypes().get(0);
        return new SecondarySortShuffler(getMapForKeyType(pairKey));
      } else if (options.getSortComparatorClass() != null) {
        RawComparator rc = ReflectionUtils.newInstance(
            options.getSortComparatorClass(),
            job.getConfiguration());
        map = new TreeMap<Object, Collection<T>>(rc);
        return new MapShuffler<S, T>(map, keyType);
      }
    }
    return new MapShuffler<S, T>(map);
  }
  
  private static class HFunction<K, V> implements Function<Map.Entry<Object, Collection<V>>, Pair<K, Iterable<V>>> {
    private final PType<K> keyType;

    public HFunction(PType<K> keyType) {
      this.keyType = keyType;
    }

    @Override
    public Pair<K, Iterable<V>> apply(Map.Entry<Object, Collection<V>> input) {
      K key;
      if (keyType == null) {
        key = (K) input.getKey();
      } else {
        Object k = keyType.getConverter().convertInput(input.getKey(), null);
        key = keyType.getInputMapFn().map(k);
      }
      return Pair.<K, Iterable<V>>of(key, new SingleUseIterable<V>(input.getValue()));
    }
  }
  
  private static class MapShuffler<K, V> extends Shuffler<K, V> {
    private final Map<Object, Collection<V>> map;
    private final PType<K> keyType;

    public MapShuffler(Map<Object, Collection<V>> map) {
      this(map, null);
    }

    public MapShuffler(Map<Object, Collection<V>> map, PType<K> keyType) {
      this.map = map;
      this.keyType = keyType;
    }
    
    @Override
    public Iterator<Pair<K, Iterable<V>>> iterator() {
      return Iterators.transform(map.entrySet().iterator(),
          new HFunction<K, V>(keyType));
    }

    @Override
    public void add(Pair<K, V> record) {
      Object key = record.first();
      if (keyType != null) {
        key = keyType.getConverter().outputKey(keyType.getOutputMapFn().map((K) key));
      }
      if (!map.containsKey(key)) {
        Collection<V> values = Lists.newArrayList();
        map.put(key, values);
      }
      map.get(key).add(record.second());
    }
  }

  private static class SSFunction<K, SK, V> implements
      Function<Map.Entry<K, List<Pair<SK, V>>>, Pair<Pair<K, SK>, Iterable<V>>> {
    @Override
    public Pair<Pair<K, SK>, Iterable<V>> apply(Entry<K, List<Pair<SK, V>>> input) {
      List<Pair<SK, V>> values = input.getValue();
      Collections.sort(values, new Comparator<Pair<SK, V>>() {
        @Override
        public int compare(Pair<SK, V> o1, Pair<SK, V> o2) {
          return ((Comparable) o1.first()).compareTo(o2.first());
        }
      });
      Pair<K, SK> key = Pair.of(input.getKey(), values.get(0).first());
      return Pair.of(key, Iterables.transform(values, new Function<Pair<SK, V>, V>() {
        @Override
        public V apply(Pair<SK, V> input) {
          return input.second();
        }
      }));
    }
  }

  private static class SecondarySortShuffler<K, SK, V> extends Shuffler<Pair<K, SK>, V> {

    private Map<K, List<Pair<SK, V>>> map;
    
    public SecondarySortShuffler(Map<K, List<Pair<SK, V>>> map) {
      this.map = map;
    }
    
    @Override
    public Iterator<Pair<Pair<K, SK>, Iterable<V>>> iterator() {
      return Iterators.transform(map.entrySet().iterator(), new SSFunction<K, SK, V>());
    }

    @Override
    public void add(Pair<Pair<K, SK>, V> record) {
      K primary = record.first().first();
      if (!map.containsKey(primary)) {
        map.put(primary, Lists.<Pair<SK, V>>newArrayList());
      }
      map.get(primary).add(Pair.of(record.first().second(), record.second()));
    }
  }
}
