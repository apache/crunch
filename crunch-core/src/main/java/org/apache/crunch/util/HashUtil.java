/*
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

package org.apache.crunch.util;

/**
 * Utility methods for working with hash codes.
 */
public class HashUtil {

  /**
   * Applies a supplemental hashing function to an integer, increasing variability in lower-order bits.
   * This method is intended to avoid collisions in functions which rely on variance in the lower bits of a hash
   * code (e.g. hash partitioning).
   */
  // The following comments and code are taken directly from Guava's com.google.common.collect.Hashing class
  // This method was written by Doug Lea with assistance from members of JCP
  // JSR-166 Expert Group and released to the public domain, as explained at
  // http://creativecommons.org/licenses/publicdomain
  //
  // As of 2010/06/11, this method is identical to the (package private) hash
  // method in OpenJDK 7's java.util.HashMap class.
  public static int smearHash(int hashCode) {
    hashCode ^= (hashCode >>> 20) ^ (hashCode >>> 12);
    return hashCode ^ (hashCode >>> 7) ^ (hashCode >>> 4);
  }
}
