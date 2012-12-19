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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Base class for the common case {@code Extractor} instances that construct a single
 * object from a block of text stored in a {@code String}, with support for error handling
 * and reporting. 
 */
public abstract class AbstractSimpleExtractor<T> implements Extractor<T> {

  private static final Log LOG = LogFactory.getLog(AbstractSimpleExtractor.class);
  private static final int LOG_ERROR_LIMIT = 100;
  
  private int errors;
  private boolean errorOnLast;
  private final T defaultValue;
  private final TokenizerFactory scannerFactory;
  
  public AbstractSimpleExtractor(T defaultValue) {
    this(defaultValue, TokenizerFactory.getDefaultInstance());
  }
  
  public AbstractSimpleExtractor(T defaultValue, TokenizerFactory scannerFactory) {
    this.defaultValue = defaultValue;
    this.scannerFactory = scannerFactory;
  }

  @Override
  public void initialize() {
    this.errors = 0;
    this.errorOnLast = false;
  }
  
  @Override
  public T extract(String input) {
    errorOnLast = false;
    T res = defaultValue;
    try {
      res = doExtract(scannerFactory.create(input));
    } catch (Exception e) {
      errorOnLast = true;
      errors++;
      if (errors < LOG_ERROR_LIMIT) {
        String msg = String.format("Error occurred parsing input '%s' using extractor %s",
            input, this); 
        LOG.error(msg);
      }
    }
    return res;
  }

  @Override
  public boolean errorOnLastRecord() {
    return errorOnLast;
  }
  
  @Override
  public T getDefaultValue() {
    return defaultValue;
  }
  
  @Override
  public ExtractorStats getStats() {
    return new ExtractorStats(errors);
  }
  
  /**
   * Subclasses must override this method to return a new instance of the
   * class that this {@code Extractor} instance is designed to parse.
   * <p>Any runtime parsing exceptions from the given {@code Tokenizer} instance
   * should be thrown so that they may be caught by the error handling logic
   * inside of this class.
   * 
   * @param tokenizer The {@code Tokenizer} instance for the current record
   * @return A new instance of the type defined for this class
   */
  protected abstract T doExtract(Tokenizer tokenizer);
}
