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

/**
 * Specifies the specific behavior of how a join should be performed in terms of requiring matching keys 
 * on both sides of the join.
 */
public enum JoinType {
  /**
   * Join two tables on a common key. Every value in the left-side table under a given key will be 
   * present with every value from the right-side table with the same key.
   * 
   * @see <a href="http://en.wikipedia.org/wiki/Join_(SQL)#Inner_join">Inner Join</a>
   */
  INNER_JOIN,
  
  /**
   * Join two tables on a common key, including entries from the left-side table that have
   * no matching key in the right-side table.
   * <p>
   * This is an optional method for implementations. 
   * 
   * @see <a href="http://en.wikipedia.org/wiki/Join_(SQL)#Left_outer_join">Left Join</a>
   */
  LEFT_OUTER_JOIN,
  
  /**
   * Join two tables on a common key, including entries from the right-side table that have
   * no matching key in the left-side table.
   * <p>
   * This is an optional method for implementations.
   * 
   * @see <a href="http://en.wikipedia.org/wiki/Join_(SQL)#Right_outer_join">Right Join</a>
   */
  RIGHT_OUTER_JOIN,
  
  /**
   * Join two tables on a common key, also including entries from both tables that have no
   * matching key in the other table.
   * <p>
   * This is an optional method for implementations.
   * 
   * @see <a href="http://en.wikipedia.org/wiki/Join_(SQL)#Full_outer_join">Full Join</a>
   */
  FULL_OUTER_JOIN
}
