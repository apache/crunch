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
package org.apache.crunch.io.text;

import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;
import org.apache.crunch.fn.CompositeMapFn;
import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

/**
 * An abstraction for parsing the lines of a text file using a {@code PType<T>} to
 * convert the lines of text into a given data type. 
 *
 * @param <T> The type returned by the text parsing
 */
abstract class LineParser<T> {

  public static <S> LineParser<S> forType(PType<S> ptype) {
    return new SimpleLineParser<S>(ptype);
  }
  
  public static <K, V> LineParser<Pair<K, V>> forTableType(PTableType<K, V> ptt, String sep) {
    return new KeyValueLineParser<K, V>(ptt, sep); 
  }
  
  private MapFn<String, T> mapFn;
  
  public void initialize() {
    mapFn = getMapFn();
    mapFn.initialize();
  }
    
  public T parse(String line) {
    return mapFn.map(line);
  }
  
  protected abstract MapFn<String, T> getMapFn();
  
  private static <T> MapFn<String, T> getMapFnForPType(PType<T> ptype) {
    MapFn ret = null;
    if (String.class.equals(ptype.getTypeClass())) {
      ret = (MapFn) IdentityFn.getInstance();
    } else {
      // Check for a composite MapFn for the PType.
      // Note that this won't work for Avro-- need to solve that.
      ret = ptype.getInputMapFn();
      if (ret instanceof CompositeMapFn) {
        ret = ((CompositeMapFn) ret).getSecond();
      }
    }
    return ret;
  }
  
  private static class SimpleLineParser<S> extends LineParser<S> {

    private final PType<S> ptype;
    
    public SimpleLineParser(PType<S> ptype) {
      this.ptype = ptype;
    }

    @Override
    protected MapFn<String, S> getMapFn() {
      return getMapFnForPType(ptype);
    }
  }
  
  private static class KeyValueLineParser<K, V> extends LineParser<Pair<K, V>> {

    private final PTableType<K, V> ptt;
    private final String sep;
    
    public KeyValueLineParser(PTableType<K, V> ptt, String sep) {
      this.ptt = ptt;
      this.sep = sep;
    }

    @Override
    protected MapFn<String, Pair<K, V>> getMapFn() {
      final MapFn<String, K> keyMapFn = getMapFnForPType(ptt.getKeyType());
      final MapFn<String, V> valueMapFn = getMapFnForPType(ptt.getValueType());
      
      return new MapFn<String, Pair<K, V>>() {
        @Override
        public void initialize() {
          keyMapFn.initialize();
          valueMapFn.initialize();
        }
        
        @Override
        public Pair<K, V> map(String input) {
          List<String> kv = ImmutableList.copyOf(Splitter.on(sep).limit(1).split(input));
          if (kv.size() != 2) {
            throw new RuntimeException("Invalid input string: " + input);
          }
          return Pair.of(keyMapFn.map(kv.get(0)), valueMapFn.map(kv.get(1)));
        }
      };
    }
  }
}
