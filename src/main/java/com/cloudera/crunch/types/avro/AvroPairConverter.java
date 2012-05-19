/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.crunch.types.avro;

import java.util.Iterator;

import com.cloudera.crunch.types.Converter;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;

import com.cloudera.crunch.Pair;
import com.cloudera.crunch.types.Converter;

public class AvroPairConverter<K, V> implements Converter<AvroKey<K>, AvroValue<V>, Pair<K, V>, Pair<K, Iterable<V>>> {

  private transient AvroKey<K> keyWrapper = null;
  private transient AvroValue<V> valueWrapper = null;
  
  @Override
  public Pair<K, V> convertInput(AvroKey<K> key, AvroValue<V> value) {
    return Pair.of(key.datum(), value.datum());
  }

  public Pair<K, Iterable<V>> convertIterableInput(AvroKey<K> key, Iterable<AvroValue<V>> iter) {
    Iterable<V> it = new AvroWrappedIterable<V>(iter);
    return Pair.of(key.datum(), it);  
  }
  
  @Override
  public AvroKey<K> outputKey(Pair<K, V> value) {
    getKeyWrapper().datum(value.first());
    return keyWrapper;
  }

  @Override
  public AvroValue<V> outputValue(Pair<K, V> value) {
    getValueWrapper().datum(value.second());
    return valueWrapper;
  }

  @Override
  public Class<AvroKey<K>> getKeyClass() {
    return (Class<AvroKey<K>>) getKeyWrapper().getClass();
  }

  @Override
  public Class<AvroValue<V>> getValueClass() {
    return (Class<AvroValue<V>>) getValueWrapper().getClass();
  }
  
  private AvroKey<K> getKeyWrapper() {
    if (keyWrapper == null) {
      keyWrapper = new AvroKey<K>();
    }
    return keyWrapper;
  }
  
  private AvroValue<V> getValueWrapper() {
    if (valueWrapper == null) {
      valueWrapper = new AvroValue<V>();
    }
    return valueWrapper;
  }
  
  private static class AvroWrappedIterable<V> implements Iterable<V> {

    private final Iterable<AvroValue<V>> iters;
    
    public AvroWrappedIterable(Iterable<AvroValue<V>> iters) {
      this.iters = iters;
    }
    
    @Override
    public Iterator<V> iterator() {
      return new Iterator<V>() {
        private final Iterator<AvroValue<V>> it = iters.iterator();

        @Override
        public boolean hasNext() {
          return it.hasNext();
        }

        @Override
        public V next() {
          return it.next().datum();
        }

        @Override
        public void remove() {
          it.remove();
        }  
      };
    }
  }
}
