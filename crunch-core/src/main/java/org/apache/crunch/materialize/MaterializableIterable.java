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
import org.apache.crunch.SourceTarget;
import org.apache.crunch.io.PathTarget;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.io.impl.FileSourceImpl;
import org.apache.hadoop.fs.Path;

public class MaterializableIterable<E> implements Iterable<E> {

  private static final Log LOG = LogFactory.getLog(MaterializableIterable.class);

  private final Pipeline pipeline;
  private final ReadableSource<E> source;
  private Iterable<E> materialized;

  public MaterializableIterable(Pipeline pipeline, ReadableSource<E> source) {
    this.pipeline = pipeline;
    this.source = source;
    this.materialized = null;
  }

  public ReadableSource<E> getSource() {
    return source;
  }

  public boolean isSourceTarget() {
    return (source instanceof SourceTarget);
  }
  
  public Path getPath() {
    if (source instanceof FileSourceImpl) {
      return ((FileSourceImpl) source).getPath();
    } else if (source instanceof PathTarget) {
      return ((PathTarget) source).getPath();
    }
    return null;
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
      materialized = source.read(pipeline.getConfiguration());
    } catch (IOException e) {
      LOG.error("Could not materialize: " + source, e);
      throw new CrunchRuntimeException(e);
    }
  }
}
