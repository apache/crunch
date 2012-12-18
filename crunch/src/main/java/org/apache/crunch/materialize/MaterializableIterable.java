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
package org.apache.crunch.materialize;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.Pipeline;
import org.apache.crunch.io.ReadableSourceTarget;

public class MaterializableIterable<E> implements Iterable<E> {

  private static final Log LOG = LogFactory.getLog(MaterializableIterable.class);

  private final Pipeline pipeline;
  private final ReadableSourceTarget<E> sourceTarget;
  private Iterable<E> materialized;

  public MaterializableIterable(Pipeline pipeline, ReadableSourceTarget<E> source) {
    this.pipeline = pipeline;
    this.sourceTarget = source;
    this.materialized = null;
  }

  public ReadableSourceTarget<E> getSourceTarget() {
    return sourceTarget;
  }

  @Override
  public Iterator<E> iterator() {
    if (materialized == null) {
      pipeline.run();
      materialize();
    }
    return materialized.iterator();
  }

  public void materialize() {
    try {
      materialized = sourceTarget.read(pipeline.getConfiguration());
    } catch (IOException e) {
      LOG.error("Could not materialize: " + sourceTarget, e);
      throw new CrunchRuntimeException(e);
    }
  }
}
