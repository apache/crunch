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

import java.util.Arrays;

import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * A {@link Tuple} instance for an arbitrary number of values.
 */
public class TupleN implements Tuple {

  private final Object values[];

  public TupleN(Object... values) {
    this.values = new Object[values.length];
    System.arraycopy(values, 0, this.values, 0, values.length);
  }

  public Object get(int index) {
    return values[index];
  }

  public int size() {
    return values.length;
  }
  
  @Override
  public int hashCode() {
  	HashCodeBuilder hcb = new HashCodeBuilder();
  	for (Object v : values) {
  	  hcb.append(v);
  	}
  	return hcb.toHashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    TupleN other = (TupleN) obj;
    return Arrays.equals(this.values, other.values);
  }

  @Override
  public String toString() {
    return Arrays.toString(values);
  }
}
