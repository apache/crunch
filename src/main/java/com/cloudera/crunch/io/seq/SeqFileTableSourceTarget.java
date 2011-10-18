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
package com.cloudera.crunch.io.seq;

import java.io.IOException;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.cloudera.crunch.Pair;
import com.cloudera.crunch.SourceTarget;
import com.cloudera.crunch.TableSource;
import com.cloudera.crunch.io.MapReduceTarget;
import com.cloudera.crunch.io.OutputHandler;
import com.cloudera.crunch.io.PathTarget;
import com.cloudera.crunch.io.SourceTargetHelper;
import com.cloudera.crunch.type.PTableType;
import com.cloudera.crunch.type.PType;

public class SeqFileTableSourceTarget<K, V> implements TableSource<K, V>,
    SourceTarget<Pair<K, V>>, PathTarget, MapReduceTarget {

  private static final Log LOG = LogFactory.getLog(SeqFileTableSourceTarget.class);
  
  private final Path path;
  private final PTableType<K, V> tableType;
  
  public SeqFileTableSourceTarget(String path, PTableType<K, V> tableType) {
    this(new Path(path), tableType);
  }
  
  public SeqFileTableSourceTarget(Path path, PTableType<K, V> tableType) {
    this.path = path;
    this.tableType = tableType;
  }
  
  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof SeqFileTableSourceTarget)) {
      return false;
    }
    SeqFileTableSourceTarget o = (SeqFileTableSourceTarget) other;
    return tableType.equals(o.tableType) && path.equals(o.path);
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(tableType).append(path).toHashCode();
  }
  
  @Override
  public String toString() {
    return "SeqTableSourceTarget(" + path.toString() + ")";
  }

  @Override
  public Path getPath() {
    return path;
  }

  @Override
  public void configureSource(Job job, int inputId) throws IOException {
    SourceTargetHelper.configureSource(job, inputId, SequenceFileInputFormat.class, path);
  }

  @Override
  public PType<Pair<K, V>> getType() {
    return tableType;
  }
  
  @Override
  public PTableType<K, V> getTableType() {
    return tableType;
  }

  @Override
  public long getSize(Configuration configuration) {
    try {
      return SourceTargetHelper.getPathSize(configuration, path);
    } catch (IOException e) {
      LOG.info(String.format("Exception thrown looking up size of: %s", path), e);
    }
    return 1L;
  }

  @Override
  public boolean accept(OutputHandler handler, PType<?> ptype) {
    handler.configure(this, ptype);
    return true;
  }

  @Override
  public void configureForMapReduce(Job job, PType<?> ptype, Path outputPath,
      String name) {
    SourceTargetHelper.configureTarget(job, SequenceFileOutputFormat.class,
        ptype.getDataBridge(), outputPath, name);
  }

}
