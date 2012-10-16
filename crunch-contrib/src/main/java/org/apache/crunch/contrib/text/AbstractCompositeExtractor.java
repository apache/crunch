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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Base class for {@code Extractor} instances that delegates the parsing of fields to other
 * {@code Extractor} instances, primarily used for constructing composite records that implement
 * the {@code Tuple} interface.
 */
public abstract class AbstractCompositeExtractor<T> implements Extractor<T> {

  private final TokenizerFactory tokenizerFactory;
  private int errors = 0;
  private boolean errorOnLast;
  private final List<Extractor<?>> extractors;
  
  public AbstractCompositeExtractor(TokenizerFactory scannerFactory, List<Extractor<?>> extractors) {
    Preconditions.checkArgument(extractors.size() > 0);
    this.tokenizerFactory = scannerFactory;
    this.extractors = extractors;
  }

  @Override
  public T extract(String input) {
    errorOnLast = false;
    Tokenizer tokenizer = tokenizerFactory.create(input);
    Object[] values = new Object[extractors.size()];
    try {
      for (int i = 0; i < values.length; i++) {
        values[i] = extractors.get(i).extract(tokenizer.next());
        if (extractors.get(i).errorOnLastRecord() && !errorOnLast) {
          errors++;
          errorOnLast = true;
        }
      }
    } catch (Exception e) {
      if (!errorOnLast) {
        errors++;
        errorOnLast = true;
      }
      return getDefaultValue();
    }
    
    return doCreate(values);
  }
  
  @Override
  public void initialize() {
    this.errors = 0;
    this.errorOnLast = false;
    for (Extractor<?> x : extractors) {
      x.initialize();
    }
  }
  
  @Override
  public boolean errorOnLastRecord() {
    return errorOnLast;
  }
  
  @Override
  public ExtractorStats getStats() {
    return new ExtractorStats(errors, Lists.transform(extractors, new Function<Extractor<?>, Integer>() {
      @Override
      public Integer apply(Extractor<?> input) {
        return input.getStats().getErrorCount();
      }
    }));
  }
  
  /**
   * Subclasses should return a new instance of the object based on the fields parsed by
   * the {@code Extractor} instances for this composite {@code Extractor} instance.
   * 
   * @param values The values that were extracted by the component {@code Extractor} objects
   * @return A new instance of the composite class for this {@code Extractor}
   */
  protected abstract T doCreate(Object[] values);
}
