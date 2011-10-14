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
public abstract class Tuple {

  public static enum TupleType { PAIR, TUPLE3, TUPLE4, TUPLEN }

  public static Tuple tuplify(TupleType type, Object... values) {
    switch(type) {
    case PAIR:
      return Pair.of(values[0], values[1]);
    case TUPLE3:
      return new Tuple3(values[0], values[1], values[2]);
    case TUPLE4:
      return new Tuple4(values[0], values[1], values[2], values[3]);
    case TUPLEN:
    default:
      return new TupleN(values);
    }
  }

  public static Tuple tuplify(Object... values) {
    switch (values.length) {
    case 2:
      return tuplify(TupleType.PAIR, values);
    case 3:
      return tuplify(TupleType.TUPLE3, values);
    case 4:
      return tuplify(TupleType.TUPLE4, values);
    default:
      return tuplify(TupleType.TUPLEN, values);
    }
  }


  
  /**
   * Returns the Object at the given index.
   */
  public abstract Object get(int index);

  /**
   * Returns the number of elements in this Tuple.
   */
  public abstract int size();
}