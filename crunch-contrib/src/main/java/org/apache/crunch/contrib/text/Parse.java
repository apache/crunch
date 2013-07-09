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
package org.apache.crunch.contrib.text;

import java.util.List;

import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;

/**
 * Methods for parsing instances of {@code PCollection<String>} into {@code PCollection}'s of strongly-typed
 * tuples.
 */
public final class Parse {

  /**
   * Parses the lines of the input {@code PCollection<String>} and returns a {@code PCollection<T>} using
   * the given {@code Extractor<T>}.
   * 
   * @param groupName A label to use for tracking errors related to the parsing process
   * @param input The input {@code PCollection<String>} to convert
   * @param extractor The {@code Extractor<T>} that converts each line
   * @return A {@code PCollection<T>}
   */
  public static <T> PCollection<T> parse(String groupName, PCollection<String> input,
      Extractor<T> extractor) {
    return parse(groupName, input, input.getTypeFamily(), extractor);
  }
  
  /**
   * Parses the lines of the input {@code PCollection<String>} and returns a {@code PCollection<T>} using
   * the given {@code Extractor<T>} that uses the given {@code PTypeFamily}.
   * 
   * @param groupName A label to use for tracking errors related to the parsing process
   * @param input The input {@code PCollection<String>} to convert
   * @param ptf The {@code PTypeFamily} of the returned {@code PCollection<T>}
   * @param extractor The {@code Extractor<T>} that converts each line
   * @return A {@code PCollection<T>}
   */
  public static <T> PCollection<T> parse(String groupName, PCollection<String> input, PTypeFamily ptf,
      Extractor<T> extractor) {
    return input.parallelDo(groupName, new ExtractorFn<T>(groupName, extractor), extractor.getPType(ptf));
  }

  /**
   * Parses the lines of the input {@code PCollection<String>} and returns a {@code PTable<K, V>} using
   * the given {@code Extractor<Pair<K, V>>}.
   * 
   * @param groupName A label to use for tracking errors related to the parsing process
   * @param input The input {@code PCollection<String>} to convert
   * @param extractor The {@code Extractor<Pair<K, V>>} that converts each line
   * @return A {@code PTable<K, V>}
   */
  public static <K, V> PTable<K, V> parseTable(String groupName, PCollection<String> input,
      Extractor<Pair<K, V>> extractor) {
    return parseTable(groupName, input, input.getTypeFamily(), extractor);
  }
  
  /**
   * Parses the lines of the input {@code PCollection<String>} and returns a {@code PTable<K, V>} using
   * the given {@code Extractor<Pair<K, V>>} that uses the given {@code PTypeFamily}.
   * 
   * @param groupName A label to use for tracking errors related to the parsing process
   * @param input The input {@code PCollection<String>} to convert
   * @param ptf The {@code PTypeFamily} of the returned {@code PTable<K, V>}
   * @param extractor The {@code Extractor<Pair<K, V>>} that converts each line
   * @return A {@code PTable<K, V>}
   */
  public static <K, V> PTable<K, V> parseTable(String groupName, PCollection<String> input,
      PTypeFamily ptf, Extractor<Pair<K, V>> extractor) {
    List<PType> st = extractor.getPType(ptf).getSubTypes();
    PTableType<K, V> ptt = ptf.tableOf((PType<K>) st.get(0), (PType<V>) st.get(1));
    return input.parallelDo(groupName, new ExtractorFn<Pair<K, V>>(groupName, extractor), ptt);
  }
  
  private static class ExtractorFn<T> extends MapFn<String, T> {

    private final String groupName;
    private final Extractor<T> extractor;
    
    public ExtractorFn(String groupName, Extractor<T> extractor) {
      this.groupName = groupName;
      this.extractor = extractor;
    }
    
    @Override
    public void initialize() {
      extractor.initialize();
    }
    
    @Override
    public T map(String input) {
      return extractor.extract(input);
    }
    
    @Override
    public void cleanup(Emitter<T> emitter) {
      if (getContext() != null) {
        ExtractorStats stats = extractor.getStats();
        increment(groupName, "OVERALL_ERRORS", stats.getErrorCount());
        List<Integer> fieldErrors = stats.getFieldErrors();
        for (int i = 0; i < fieldErrors.size(); i++) {
          increment(groupName, "ERRORS_FOR_FIELD_" + i, fieldErrors.get(i));
        }
      }
    }
  }
  
  // Non-instantiable.
  private Parse() { }
}
