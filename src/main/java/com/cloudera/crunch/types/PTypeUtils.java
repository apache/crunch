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
package com.cloudera.crunch.types;

import java.util.Collection;
import java.util.List;

import com.cloudera.crunch.Pair;
import com.cloudera.crunch.Tuple;
import com.cloudera.crunch.Tuple3;
import com.cloudera.crunch.Tuple4;
import com.cloudera.crunch.TupleN;

/**
 * Utilities for converting between {@code PType}s from different {@code PTypeFamily}
 * implementations.
 *
 */
public class PTypeUtils {

  public static <T> PType<T> convert(PType<T> ptype, PTypeFamily tf) {
    if (ptype instanceof PTableType) {
      PTableType ptt = (PTableType) ptype;
      return tf.tableOf(tf.as(ptt.getKeyType()), tf.as(ptt.getValueType()));
    }
    Class<T> typeClass = ptype.getTypeClass();
    if (Tuple.class.isAssignableFrom(typeClass)) {
      List<PType> subTypes = ptype.getSubTypes();
      if (Pair.class.equals(typeClass)) {
        return tf.pairs(tf.as(subTypes.get(0)), tf.as(subTypes.get(1)));
      } else if (Tuple3.class.equals(typeClass)) {
        return tf.triples(tf.as(subTypes.get(0)), tf.as(subTypes.get(1)), tf.as(subTypes.get(2)));
      } else if (Tuple4.class.equals(typeClass)) {
        return tf.quads(tf.as(subTypes.get(0)), tf.as(subTypes.get(1)),
            tf.as(subTypes.get(2)), tf.as(subTypes.get(3)));
      } else if (TupleN.class.equals(typeClass)) {
        PType[] newPTypes = subTypes.toArray(new PType[0]);
        for (int i = 0; i < newPTypes.length; i++) {
          newPTypes[i] = tf.as(subTypes.get(i));
        }
        return (PType<T>) tf.tuples(newPTypes);
      }
    }
    if (Collection.class.isAssignableFrom(typeClass)) {
      return tf.collections(tf.as(ptype.getSubTypes().get(0)));
    }
    return tf.records(typeClass);
  }
  
  private PTypeUtils() {}
}
