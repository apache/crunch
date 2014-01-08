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
package org.apache.crunch;

/**
 * Allows us to represent the combination of multiple data sources that may contain different types of data
 * as a single type with an index to indicate which of the original sources the current record was from.
 */
public class Union {

  private final int index;
  private final Object value;

  public Union(int index, Object value) {
    this.index = index;
    this.value = value;
  }

  /**
   * Returns the index of the original data source for this union type.
   */
  public int getIndex() {
    return index;
  }

  /**
   * Returns the underlying object value of the record.
   */
  public Object getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Union that = (Union) o;

    if (index != that.index) return false;
    if (value != null ? !value.equals(that.value) : that.value != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return 31 * index + (value != null ? value.hashCode() : 0);
  }
}
