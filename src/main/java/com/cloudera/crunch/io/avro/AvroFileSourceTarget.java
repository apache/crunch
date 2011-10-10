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
package com.cloudera.crunch.io.avro;

import java.io.IOException;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import com.cloudera.crunch.SourceTarget;
import com.cloudera.crunch.io.SourceTargetHelper;
import com.cloudera.crunch.type.PType;
import com.cloudera.crunch.type.avro.AvroInputFormat;
import com.cloudera.crunch.type.avro.AvroType;

public class AvroFileSourceTarget<T> extends AvroFileTarget implements SourceTarget<T> {

  private static final Log LOG = LogFactory.getLog(AvroFileSourceTarget.class);
  
  private final AvroType<T> ptype;
  
  public AvroFileSourceTarget(Path path, AvroType<T> ptype) {
    super(path);
    this.ptype = ptype;
  }
  
  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof AvroFileSourceTarget)) {
      return false;
    }
    AvroFileSourceTarget o = (AvroFileSourceTarget) other;
    return ptype.equals(o.ptype) && path.equals(o.path);
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(ptype).append(path).toHashCode();
  }
  
  @Override
  public PType<T> getType() {
    return ptype;
  }

  @Override
  public String toString() {
    return "AvroSourceTarget(" + path.toString() + ")";
  }

  @Override
  public void configureSource(Job job, int inputId) throws IOException {
    SourceTargetHelper.configureSource(job, inputId, AvroInputFormat.class, path);
    
    Configuration conf = job.getConfiguration();
    String inputSchema = conf.get("avro.input.schema");
    if (inputSchema == null) {
      conf.set("avro.input.schema", ptype.getSchema().toString());
    } else if (!inputSchema.equals(ptype.getSchema().toString())) {
      throw new IllegalStateException("Multiple Avro sources must use the same schema");
    }
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
}
