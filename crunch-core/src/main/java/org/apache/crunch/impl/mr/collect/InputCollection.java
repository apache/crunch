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

import org.apache.crunch.ParallelDoOptions;
import org.apache.crunch.ReadableData;
import org.apache.crunch.Source;
import org.apache.crunch.impl.dist.collect.BaseInputCollection;
import org.apache.crunch.impl.dist.collect.MRCollection;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.impl.mr.plan.DoNode;
import org.apache.crunch.io.ReadableSource;

public class InputCollection<S> extends BaseInputCollection<S> implements MRCollection {

  public InputCollection(Source<S> source, String name, MRPipeline pipeline, ParallelDoOptions doOpts) {
    super(source, name, pipeline, doOpts);
  }

  @Override
  protected ReadableData<S> getReadableDataInternal() {
    if (source instanceof ReadableSource) {
      return ((ReadableSource<S>) source).asReadable();
    } else {
      return materializedData();
    }
  }

  @Override
  public DoNode createDoNode() {
    return DoNode.createInputNode(source);
  }
}
