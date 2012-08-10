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

import com.google.common.collect.Lists;

/**
 * Performs deep copies (based on underlying PType deep copying) of Collections.
 * 
 * @param <T>
 *          The type of Tuple implementation being copied
 */
public class CollectionDeepCopier<T> implements DeepCopier<Collection<T>> {

  private PType<T> elementType;

  public CollectionDeepCopier(PType<T> elementType) {
    this.elementType = elementType;
  }

  @Override
  public Collection<T> deepCopy(Collection<T> source) {
    List<T> copiedCollection = Lists.newArrayListWithCapacity(source.size());
    for (T value : source) {
      copiedCollection.add(elementType.getDetachedValue(value));
    }
    return copiedCollection;
  }

}
