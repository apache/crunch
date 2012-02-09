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
package com.cloudera.crunch.impl.mem.collect;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.Source;
import com.cloudera.crunch.TableSource;
import com.cloudera.crunch.Target;

public class MemPipeline implements Pipeline {

  private static final MemPipeline INSTANCE = new MemPipeline();
  
  public static Pipeline getInstance() {
    return INSTANCE;
  }
  
  private Configuration conf = new Configuration();

  private MemPipeline() {
    
  }
  
  @Override
  public void setConfiguration(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public <T> PCollection<T> read(Source<T> source) {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see com.cloudera.crunch.Pipeline#read(com.cloudera.crunch.TableSource)
   */
  @Override
  public <K, V> PTable<K, V> read(TableSource<K, V> tableSource) {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see com.cloudera.crunch.Pipeline#write(com.cloudera.crunch.PCollection, com.cloudera.crunch.Target)
   */
  @Override
  public void write(PCollection<?> collection, Target target) {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see com.cloudera.crunch.Pipeline#readTextFile(java.lang.String)
   */
  @Override
  public PCollection<String> readTextFile(String pathName) {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see com.cloudera.crunch.Pipeline#writeTextFile(com.cloudera.crunch.PCollection, java.lang.String)
   */
  @Override
  public <T> void writeTextFile(PCollection<T> collection, String pathName) {
    // TODO Auto-generated method stub
  }

  @Override
  public <T> Iterable<T> materialize(PCollection<T> pcollection) {
    return pcollection.materialize();
  }

  @Override
  public void run() {
  }

  @Override
  public void done() {
  }
}
