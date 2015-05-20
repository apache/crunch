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
package org.apache.crunch.impl.mem;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

class CountersWrapper extends Counters {

  private Counters active;
  private final Map<String, Map<String, Counter>> lookupCache = Maps.newHashMap();
  private Set<Counters> allCounters = Sets.newHashSet();

  CountersWrapper() {
    this.active = new Counters();
    allCounters.add(active);
  }

  CountersWrapper(org.apache.hadoop.mapred.Counters counters) {
    this.active = new Counters(counters);
    allCounters.add(active);
  }

  @Override
  public Counter findCounter(String groupName, String counterName) {
    Map<String, Counter> c = lookupCache.get(groupName);
    if (c == null) {
      c = Maps.newHashMap();
      lookupCache.put(groupName, c);
    }
    Counter counter = c.get(counterName);
    if (counter == null) {
      try {
        counter = active.findCounter(groupName, counterName);
      } catch (Exception e) {
        // Recover from this by creating a new active instance
        active = new Counters();
        allCounters.add(active);
        counter = active.findCounter(groupName, counterName);
      }
      c.put(counterName, counter);
    }
    return counter;
  }

  @Override
  public synchronized Counter findCounter(Enum<?> key) {
    return findCounter(key.getClass().getName(), key.name());
  }

  @Override
  public synchronized Collection<String> getGroupNames() {
    return lookupCache.keySet();
  }

  @Override
  public Iterator<CounterGroup> iterator() {
    return Iterators.concat(Iterables.transform(allCounters, new Function<Counters, Iterator<CounterGroup>>() {
      @Override
      public Iterator<CounterGroup> apply(Counters input) {
        return input.iterator();
      }
    }).iterator());
  }

  @Override
  public synchronized CounterGroup getGroup(String groupName) {
    if (allCounters.size() == 1) {
      return active.getGroup(groupName);
    } else {
      throw new UnsupportedOperationException(
          "CounterWrapper cannot return CounterGroup when there are too many Counters");
    }
  }

  public synchronized void write(DataOutput out) throws IOException {
    throw new UnsupportedOperationException("CountersWrapper may not be written");
  }

  public synchronized void readFields(DataInput in) throws IOException {
    throw new UnsupportedOperationException("CountersWrapper may not be read");
  }

  @Override
  public synchronized int countCounters() {
    int cntrs = 0;
    for (Counters c : allCounters) {
      cntrs += c.countCounters();
    }
    return cntrs;
  }

  public synchronized void incrAllCounters(Counters other) {
    for (CounterGroup cg : other) {
      for (Counter c : cg) {
        findCounter(cg.getName(), c.getName()).increment(c.getValue());
      }
    }
  }
}
