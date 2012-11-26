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

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.crunch.GroupingOptions;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.types.PType;
import org.apache.hadoop.io.RawComparator;
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
    Map<S, Collection<T>> map = getMapForKeyType(keyType);
    
    if (options != null) {
      if (Pair.class.equals(keyType.getTypeClass()) && options.getGroupingComparatorClass() != null) {
        PType<?> pairKey = keyType.getSubTypes().get(0);
        return new SecondarySortShuffler(getMapForKeyType(pairKey));
      } else if (options.getSortComparatorClass() != null) {
        RawComparator<S> rc = ReflectionUtils.newInstance(options.getSortComparatorClass(),
            pipeline.getConfiguration());
        map = new TreeMap<S, Collection<T>>(rc);
      }
    }
    
    return new MapShuffler<S, T>(map);
  }
  
  private static class HFunction<K, V> implements Function<Map.Entry<K, Collection<V>>, Pair<K, Iterable<V>>> {
    @Override
    public Pair<K, Iterable<V>> apply(Map.Entry<K, Collection<V>> input) {
      return Pair.of(input.getKey(), (Iterable<V>) input.getValue());
    }
  }
  
  private static class MapShuffler<K, V> extends Shuffler<K, V> {
    private final Map<K, Collection<V>> map;
    
    public MapShuffler(Map<K, Collection<V>> map) {
      this.map = map;
    }
    
    @Override
    public Iterator<Pair<K, Iterable<V>>> iterator() {
      return Iterators.transform(map.entrySet().iterator(),
          new HFunction<K, V>());
    }

    @Override
    public void add(Pair<K, V> record) {
      if (!map.containsKey(record.first())) {
        Collection<V> values = Lists.newArrayList();
        map.put(record.first(), values);
      }
      map.get(record.first()).add(record.second());
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
