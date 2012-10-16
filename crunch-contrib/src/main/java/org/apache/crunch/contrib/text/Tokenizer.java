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

import java.util.Scanner;
import java.util.Set;

/**
 * Manages a {@link Scanner} instance and provides support for returning only a subset
 * of the fields returned by the underlying {@code Scanner}.
 */
public class Tokenizer {

  private final Scanner scanner;
  private final Set<Integer> indices;
  private final boolean keep;
  private int current;
  
  /**
   * Create a new {@code Tokenizer} instance.
   * 
   * @param scanner The scanner to manage
   * @param indices The indices to keep/drop
   * @param keep Whether the indices should be kept (true) or dropped (false)
   */
  public Tokenizer(Scanner scanner, Set<Integer> indices, boolean keep) {
    this.scanner = scanner;
    this.indices = checkIndices(indices);
    this.keep = keep;
    this.current = -1;
  }
  
  private static Set<Integer> checkIndices(Set<Integer> indices) {
    for (Integer index : indices) {
      if (index < 0) {
        throw new IllegalArgumentException("All tokenizer indices must be non-negative");
      }
    }
    return indices;
  }
  
  private void advance() {
    if (indices.isEmpty()) {
      return;
    }
    current++;
    while (scanner.hasNext() &&
        (keep && !indices.contains(current)) || (!keep && indices.contains(current))) {
      scanner.next();
      current++;
    }
  }
  
  /**
   * Returns true if the underlying {@code Scanner} has any tokens remaining.
   */
  public boolean hasNext() {
    return scanner.hasNext();
  }

  /**
   * Advance this {@code Tokenizer} and return the next String from the {@code Scanner}.
   * 
   * @return The next String from the {@code Scanner}
   */
  public String next() {
    advance();
    return scanner.next();
  }

  /**
   * Advance this {@code Tokenizer} and return the next Long from the {@code Scanner}.
   * 
   * @return The next Long from the {@code Scanner}
   */
  public Long nextLong() {
    advance();
    return scanner.nextLong();
  }

  /**
   * Advance this {@code Tokenizer} and return the next Boolean from the {@code Scanner}.
   * 
   * @return The next Boolean from the {@code Scanner}
   */
  public Boolean nextBoolean() {
    advance();
    return scanner.nextBoolean();
  }

  /**
   * Advance this {@code Tokenizer} and return the next Double from the {@code Scanner}.
   * 
   * @return The next Double from the {@code Scanner}
   */
  public Double nextDouble() {
    advance();
    return scanner.nextDouble();
  }

  /**
   * Advance this {@code Tokenizer} and return the next Float from the {@code Scanner}.
   * 
   * @return The next Float from the {@code Scanner}
   */
  public Float nextFloat() {
    advance();
    return scanner.nextFloat();
  }

  /**
   * Advance this {@code Tokenizer} and return the next Integer from the {@code Scanner}.
   * 
   * @return The next Integer from the {@code Scanner}
   */
  public Integer nextInt() {
    advance();
    return scanner.nextInt();
  }
}
