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
package org.apache.crunch.lib.join;

import java.io.Serializable;

import org.apache.crunch.PTable;
import org.apache.crunch.Pair;

/**
 * Defines a strategy for joining two PTables together on a common key.
 */
public interface JoinStrategy<K, U, V> extends Serializable {
  
  /**
   * Join two tables with the given join type.
   * 
   * @param left left table to be joined
   * @param right right table to be joined
   * @param joinType type of join to perform
   * @return joined tables
   */
  PTable<K, Pair<U,V>> join(PTable<K, U> left, PTable<K, V> right, JoinType joinType);
  
}
