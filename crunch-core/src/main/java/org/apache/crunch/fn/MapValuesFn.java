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
package org.apache.crunch.fn;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

/**
 * @deprecated Use {@link org.apache.crunch.PTable#mapValues(org.apache.crunch.MapFn, org.apache.crunch.types.PType)}
 */
public abstract class MapValuesFn<K, V1, V2> extends DoFn<Pair<K, V1>, Pair<K, V2>> {

  @Override
  public void process(Pair<K, V1> input, Emitter<Pair<K, V2>> emitter) {
    emitter.emit(Pair.of(input.first(), map(input.second())));
  }

  public abstract V2 map(V1 v);
}
