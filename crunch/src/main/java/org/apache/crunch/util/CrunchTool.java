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
package org.apache.crunch.util;

import java.io.Serializable;

import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineExecution;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Source;
import org.apache.crunch.TableSource;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.At;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

/**
 * An extension of the {@code Tool} interface that creates a {@code Pipeline}
 * instance and provides methods for working with the Pipeline from inside of
 * the Tool's run method.
 * 
 */
public abstract class CrunchTool extends Configured implements Tool, Serializable {

  protected static final From from = new From();
  protected static final To to = new To();
  protected static final At at = new At();

  // Pipeline object itself isn't necessarily serializable.
  private transient Pipeline pipeline;

  public CrunchTool() {
    this(false);
  }

  public CrunchTool(boolean inMemory) {
    this.pipeline = inMemory ? MemPipeline.getInstance() : new MRPipeline(getClass());
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf != null && pipeline != null) {
      pipeline.setConfiguration(conf);
    }
  }

  @Override
  public Configuration getConf() {
    return pipeline.getConfiguration();
  }

  public void enableDebug() {
    pipeline.enableDebug();
  }

  public <T> PCollection<T> read(Source<T> source) {
    return pipeline.read(source);
  }

  public <K, V> PTable<K, V> read(TableSource<K, V> tableSource) {
    return pipeline.read(tableSource);
  }

  public PCollection<String> readTextFile(String pathName) {
    return pipeline.readTextFile(pathName);
  }

  public void write(PCollection<?> pcollection, Target target) {
    pipeline.write(pcollection, target);
  }

  public void writeTextFile(PCollection<?> pcollection, String pathName) {
    pipeline.writeTextFile(pcollection, pathName);
  }
  
  public <T> Iterable<T> materialize(PCollection<T> pcollection) {
    return pipeline.materialize(pcollection);
  }

  public PipelineResult run() {
    return pipeline.run();
  }

  public PipelineExecution runAsync() {
    return pipeline.runAsync();
  }

  public PipelineResult done() {
    return pipeline.done();
  }

  protected Pipeline getPipeline() {
    return pipeline;
  }
}
