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
package org.apache.crunch.io.avro;

import org.apache.avro.mapred.AvroWrapper;
import org.apache.crunch.impl.mr.plan.PlanningParameters;
import org.apache.crunch.io.FileNamingScheme;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.OutputHandler;
import org.apache.crunch.io.SequentialFileNamingScheme;
import org.apache.crunch.io.impl.FileTargetImpl;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroMode;
import org.apache.crunch.types.avro.AvroPathPerKeyOutputFormat;
import org.apache.crunch.types.avro.AvroType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A {@link org.apache.crunch.Target} that wraps {@link org.apache.crunch.types.avro.AvroPathPerKeyOutputFormat} to allow one file
 * per key to be written as the output of a {@code PTable<String, T>}.
 *
 * <p>Note the restrictions that apply to the {@code AvroPathPerKeyOutputFormat}; in particular, it's a good
 * idea to write out all of the records for the same key together within each partition of the data.
 */
public class AvroPathPerKeyTarget extends FileTargetImpl {

  private static final Logger LOG = LoggerFactory.getLogger(AvroPathPerKeyTarget.class);

  public AvroPathPerKeyTarget(String path) {
    this(new Path(path));
  }

  public AvroPathPerKeyTarget(Path path) {
    this(path, SequentialFileNamingScheme.getInstance());
  }

  public AvroPathPerKeyTarget(Path path, FileNamingScheme fileNamingScheme) {
    super(path, AvroPathPerKeyOutputFormat.class, fileNamingScheme);
  }

  @Override
  public boolean accept(OutputHandler handler, PType<?> ptype) {
    if (ptype instanceof PTableType && ptype instanceof AvroType) {
      if (String.class.equals(((PTableType) ptype).getKeyType().getTypeClass())) {
        handler.configure(this, ptype);
        return true;
      }
    }
    return false;
  }

  @Override
  public void configureForMapReduce(Job job, PType<?> ptype, Path outputPath, String name) {
    AvroType<?> atype = (AvroType) ((PTableType) ptype).getValueType();
    FormatBundle bundle = FormatBundle.forOutput(AvroPathPerKeyOutputFormat.class);
    String schemaParam;
    if (name == null) {
      schemaParam = "avro.output.schema";
    } else {
      schemaParam = "avro.output.schema." + name;
    }
    bundle.set(schemaParam, atype.getSchema().toString());
    AvroMode.fromType(atype).configure(bundle);
    configureForMapReduce(job, AvroWrapper.class, NullWritable.class, bundle, outputPath, name);
  }

  @Override
  public void handleOutputs(Configuration conf, Path workingPath, int index) throws IOException {
    FileSystem srcFs = workingPath.getFileSystem(conf);
    Path base = new Path(workingPath, PlanningParameters.MULTI_OUTPUT_PREFIX + index);
    if (!srcFs.exists(base)) {
      LOG.warn("Nothing to copy from {}", base);
      return;
    }
    Path[] keys = FileUtil.stat2Paths(srcFs.listStatus(base));
    FileSystem dstFs = path.getFileSystem(conf);
    if (!dstFs.exists(path)) {
      dstFs.mkdirs(path);
    }
    boolean sameFs = isCompatible(srcFs, path);
    for (Path key : keys) {
      Path[] srcs = FileUtil.stat2Paths(srcFs.listStatus(key), key);
      Path targetPath = new Path(path, key.getName());
      dstFs.mkdirs(targetPath);
      for (Path s : srcs) {
        Path d = getDestFile(conf, s, targetPath, s.getName().contains("-m-"));
        if (sameFs) {
          srcFs.rename(s, d);
        } else {
          FileUtil.copy(srcFs, s, dstFs, d, true, true, conf);
        }
      }
    }
    dstFs.create(getSuccessIndicator(), true).close();
  }

  @Override
  public String toString() {
    return "AvroFilePerKey(" + path + ")";
  }
}
