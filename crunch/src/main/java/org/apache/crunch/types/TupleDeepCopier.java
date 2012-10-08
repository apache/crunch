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

import java.util.List;

import org.apache.crunch.Tuple;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;

/**
 * Performs deep copies (based on underlying PType deep copying) of Tuple-based objects.
 * 
 * @param <T> The type of Tuple implementation being copied
 */
public class TupleDeepCopier<T extends Tuple> implements DeepCopier<T> {

  private final TupleFactory<T> tupleFactory;
  private final List<PType> elementTypes;

  public TupleDeepCopier(Class<T> tupleClass, PType... elementTypes) {
    tupleFactory = TupleFactory.getTupleFactory(tupleClass);
    this.elementTypes = Lists.newArrayList(elementTypes);
  }

  @Override
  public void initialize(Configuration conf) {
    for (PType elementType : elementTypes) {
      elementType.initialize(conf);
    }
  }

  @Override
  public T deepCopy(T source) {
    Object[] deepCopyValues = new Object[source.size()];

    for (int valueIndex = 0; valueIndex < elementTypes.size(); valueIndex++) {
      PType elementType = elementTypes.get(valueIndex);
      deepCopyValues[valueIndex] = elementType.getDetachedValue(source.get(valueIndex));
    }

    return tupleFactory.makeTuple(deepCopyValues);
  }
}
