package com.cloudera.crunch.materialize;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.cloudera.crunch.Pair;

public class MaterializableMap<K, V> extends AbstractMap<K, V> {
  
  private Iterable<Pair<K, V>> iterable;
  private Set<Map.Entry<K, V>> entrySet;
  
  public MaterializableMap(Iterable<Pair<K, V>> iterable) {
  	this.iterable = iterable;
  }
  
  private Set<Map.Entry<K, V>> toMapEntries(Iterable<Pair<K, V>> xs) {
    HashMap<K, V> m = new HashMap<K, V>();
    for (Pair<K, V> x : xs)
      m.put(x.first(), x.second());
    return m.entrySet();
  }

  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    if (entrySet == null)
      entrySet = toMapEntries(iterable);
    return entrySet;
  }

}