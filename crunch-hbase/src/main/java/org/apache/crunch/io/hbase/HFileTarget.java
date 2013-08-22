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
package org.apache.crunch.io.hbase;

import com.google.common.base.Preconditions;
import org.apache.commons.codec.binary.Hex;
import org.apache.crunch.io.CrunchOutputs;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.SequentialFileNamingScheme;
import org.apache.crunch.io.impl.FileTargetImpl;
import org.apache.crunch.types.PType;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HFileTarget extends FileTargetImpl {

  private static final HColumnDescriptor DEFAULT_COLUMN_DESCRIPTOR = new HColumnDescriptor();
  private final HColumnDescriptor hcol;

  public HFileTarget(String path) {
    this(new Path(path));
  }

  public HFileTarget(Path path) {
    this(path, DEFAULT_COLUMN_DESCRIPTOR);
  }

  public HFileTarget(Path path, HColumnDescriptor hcol) {
    super(path, HFileOutputFormatForCrunch.class, SequentialFileNamingScheme.getInstance());
    this.hcol = Preconditions.checkNotNull(hcol);
  }

  @Override
  protected void configureForMapReduce(
      Job job,
      Class keyClass,
      Class valueClass,
      Class outputFormatClass,
      Path outputPath,
      String name) {
    try {
      FileOutputFormat.setOutputPath(job, outputPath);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    String hcolStr = Hex.encodeHexString(WritableUtils.toByteArray(hcol));
    if (name == null) {
      job.setOutputFormatClass(HFileOutputFormatForCrunch.class);
      job.setOutputKeyClass(keyClass);
      job.setOutputValueClass(valueClass);
      job.getConfiguration().set(HFileOutputFormatForCrunch.HCOLUMN_DESCRIPTOR_KEY, hcolStr);
    } else {
      FormatBundle<HFileOutputFormatForCrunch> bundle = FormatBundle.forOutput(HFileOutputFormatForCrunch.class);
      bundle.set(HFileOutputFormatForCrunch.HCOLUMN_DESCRIPTOR_KEY, hcolStr);
      CrunchOutputs.addNamedOutput(job, name, bundle, keyClass, valueClass);
    }
  }

  @Override
  public String toString() {
    return "HFile(" + path + ")";
  }
}
