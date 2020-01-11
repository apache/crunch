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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import java.io.Serializable;
import java.util.Locale;
import java.util.Scanner;
import java.util.Set;

/**
 * Factory class that constructs {@link Tokenizer} instances for input strings that use a fixed
 * set of delimiters, skip patterns, locales, and sets of indices to keep or drop.
 */
public class TokenizerFactory implements Serializable {

  private static TokenizerFactory DEFAULT_INSTANCE = new TokenizerFactory(null, null, null,
      ImmutableSet.<Integer>of(), true);
  
  private final String delim;
  private final String skip;
  private final Locale locale;
  private final Set<Integer> indices;
  private final boolean keep;
  
  /**
   * Returns a default {@code TokenizerFactory} that uses whitespace as a delimiter and does
   * not skip any input fields.
   * @return The default {@code TokenizerFactory}
   */
  public static TokenizerFactory getDefaultInstance() { return DEFAULT_INSTANCE; }
  
  private TokenizerFactory(String delim, String skip, Locale locale,
      Set<Integer> indices, boolean keep) {
    this.delim = delim;
    this.skip = skip;
    this.locale = locale;
    this.indices = indices;
    this.keep = keep;
  }
  
  /**
   * Return a {@code Scanner} instance that wraps the input string and uses the delimiter,
   * skip, and locale settings for this {@code TokenizerFactory} instance.
   * 
   * @param input The input string
   * @return A new {@code Scanner} instance with appropriate settings
   */
  public Tokenizer create(String input) {
    Scanner s = new Scanner(input);
    s.useLocale(Locale.US); // Use period for floating point number formatting
    if (delim != null) {
      s.useDelimiter(delim);
    }
    if (skip != null) {
      s.skip(skip);
    }
    if (locale != null) {
      s.useLocale(locale);
    }
    return new Tokenizer(s, indices, keep);
  }

  /**
   * Factory method for creating a {@code TokenizerFactory.Builder} instance.
   * @return A new {@code TokenizerFactory.Builder}
   */
  public static Builder builder() {
    return new Builder();
  }
  
  /**
   * A class for constructing new {@code TokenizerFactory} instances using the Builder pattern.
   */
  public static class Builder {
    private String delim;
    private String skip;
    private Locale locale;
    private Set<Integer> indices = ImmutableSet.of();
    private boolean keep;
    
    /**
     * Sets the delimiter used by the {@code TokenizerFactory} instances constructed by
     * this instance.
     * @param delim The delimiter to use, which may be a regular expression
     * @return This {@code Builder} instance
     */
    public Builder delimiter(String delim) {
      this.delim = delim;
      return this;
    }
    
    /**
     * Sets the regular expression that determines which input characters should be
     * ignored by the {@code Scanner} that is returned by the constructed
     * {@code TokenizerFactory}.
     * 
     * @param skip The regular expression of input values to ignore
     * @return This {@code Builder} instance
     */
    public Builder skip(String skip) {
      this.skip = skip;
      return this;
    }
    
    /**
     * Sets the {@code Locale} to use with the {@code TokenizerFactory} returned by
     * this {@code Builder} instance.
     * 
     * @param locale The locale to use
     * @return This {@code Builder} instance
     */
    public Builder locale(Locale locale) {
      this.locale = locale;
      return this;
    }
    
    /**
     * Keep only the specified fields found by the input scanner, counting from
     * zero.
     * 
     * @param indices The indices to keep
     * @return This {@code Builder} instance
     */
    public Builder keep(Integer... indices) {
      Preconditions.checkArgument(this.indices.isEmpty(),
          "Cannot set keep indices more than once");
      this.indices = ImmutableSet.copyOf(indices);
      this.keep = true;
      return this;
    }
    
    /**
     * Drop the specified fields found by the input scanner, counting from zero.
     * 
     * @param indices The indices to drop
     * @return This {@code Builder} instance
     */
    public Builder drop(Integer... indices) {
      Preconditions.checkArgument(this.indices.isEmpty(),
          "Cannot set drop indices more than once");
      this.indices = ImmutableSet.copyOf(indices);
      this.keep = false;
      return this;
    }
    
    /**
     * Returns a new {@code TokenizerFactory} with settings determined by this
     * {@code Builder} instance.
     * @return A new {@code TokenizerFactory}
     */
    public TokenizerFactory build() {
      return new TokenizerFactory(delim, skip, locale, indices, keep);
    }
  }
  
}
