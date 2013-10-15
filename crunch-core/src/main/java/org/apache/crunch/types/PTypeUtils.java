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
package org.apache.crunch.types;

import java.util.Collection;
import java.util.List;

import org.apache.crunch.Pair;
import org.apache.crunch.Tuple;
import org.apache.crunch.Tuple3;
import org.apache.crunch.Tuple4;
import org.apache.crunch.TupleN;

/**
 * Utilities for converting between {@code PType}s from different
 * {@code PTypeFamily} implementations.
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
        return tf.quads(tf.as(subTypes.get(0)), tf.as(subTypes.get(1)), tf.as(subTypes.get(2)), tf.as(subTypes.get(3)));
      } else if (TupleN.class.equals(typeClass)) {
        PType[] newPTypes = subTypes.toArray(new PType[subTypes.size()]);
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

  private PTypeUtils() {
  }
}
