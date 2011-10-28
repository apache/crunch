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

package com.cloudera.crunch;

/**
 * A fixed-size collection of Objects, used in Crunch for representing
 * joins between {@code PCollection}s.
 *
 */
public interface Tuple {

  /**
   * Returns the Object at the given index.
   */
  Object get(int index);

  /**
   * Returns the number of elements in this Tuple.
   */
  int size();
}