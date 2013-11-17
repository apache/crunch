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
package org.apache.crunch.impl.mr.collect;

import org.apache.crunch.TableSource;
import org.apache.crunch.impl.dist.collect.BaseInputTable;
import org.apache.crunch.impl.dist.collect.MRCollection;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.impl.mr.plan.DoNode;

public class InputTable<K, V> extends BaseInputTable<K, V> implements MRCollection {

  public InputTable(TableSource<K, V> source, MRPipeline pipeline) {
    super(source, pipeline);
  }

  @Override
  public DoNode createDoNode() {
    return DoNode.createInputNode(source);
  }
}
