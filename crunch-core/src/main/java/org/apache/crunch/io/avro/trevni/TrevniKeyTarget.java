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
package org.apache.crunch.io.avro.trevni;

import org.apache.avro.hadoop.io.AvroKeyComparator;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.impl.mr.plan.PlanningParameters;
import org.apache.crunch.io.CrunchOutputs;
import org.apache.crunch.io.FileNamingScheme;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.OutputHandler;
import org.apache.crunch.io.SequentialFileNamingScheme;
import org.apache.crunch.io.impl.FileTargetImpl;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.Collection;

import static org.apache.crunch.types.avro.Avros.REFLECT_DATA_FACTORY;
import static org.apache.crunch.types.avro.Avros.REFLECT_DATA_FACTORY_CLASS;

public class TrevniKeyTarget extends FileTargetImpl {

  public TrevniKeyTarget(String path) {
    this(new Path(path));
  }

  public TrevniKeyTarget(Path path) {
    this(path, SequentialFileNamingScheme.getInstance());
  }

  public TrevniKeyTarget(Path path, FileNamingScheme fileNamingScheme) {
    super(path, TrevniOutputFormat.class, fileNamingScheme);
  }

  @Override
  public String toString() {
    return "TrevniKey(" + path.toString() + ")";
  }

  @Override
  public boolean accept(OutputHandler handler, PType<?> ptype) {
    if (!(ptype instanceof AvroType)) {
      return false;
    }
    handler.configure(this, ptype);
    return true;
  }

  @Override
  public void configureForMapReduce(Job job, PType<?> ptype, Path outputPath, String name) {
    AvroType<?> atype = (AvroType<?>) ptype;
    Configuration conf = job.getConfiguration();

    if (null == name) {
      AvroJob.setOutputKeySchema(job, atype.getSchema());
      AvroJob.setMapOutputKeySchema(job, atype.getSchema());

      Avros.configureReflectDataFactory(conf);
      configureForMapReduce(job, AvroKey.class, NullWritable.class, FormatBundle.forOutput(TrevniOutputFormat.class),
          outputPath, null);
    } else {
      FormatBundle<TrevniOutputFormat> bundle = FormatBundle.forOutput(
          TrevniOutputFormat.class);

      bundle.set("avro.schema.output.key", atype.getSchema().toString());
      bundle.set("mapred.output.value.groupfn.class", AvroKeyComparator.class.getName());
      bundle.set("mapred.output.key.comparator.class", AvroKeyComparator.class.getName());
      bundle.set("avro.serialization.key.writer.schema", atype.getSchema().toString());
      bundle.set("avro.serialization.key.reader.schema", atype.getSchema().toString());

      //Equivalent to...
      // AvroSerialization.addToConfiguration(job.getConfiguration());
      Collection<String> serializations = conf.getStringCollection("io.serializations");
      if (!serializations.contains(AvroSerialization.class.getName())) {
        serializations.add(AvroSerialization.class.getName());
        bundle.set(name, StringUtils.arrayToString(serializations.toArray(new String[serializations.size()])));
      }

      //The following is equivalent to Avros.configureReflectDataFactory(conf);
      bundle.set(REFLECT_DATA_FACTORY_CLASS, REFLECT_DATA_FACTORY.getClass().getName());

      //Set output which honors the name.
      bundle.set("mapred.output.dir", new Path(outputPath, name).toString());

      //Set value which will be ignored but should get past the FileOutputFormat.checkOutputSpecs(..)
      //which requires the "mapred.output.dir" value to be set.
      try{
        FileOutputFormat.setOutputPath(job, outputPath);
      } catch(Exception ioe){
          throw new RuntimeException(ioe);
      }

      CrunchOutputs.addNamedOutput(job, name,
          bundle,
          AvroKey.class,
          NullWritable.class);
    }
  }

  @Override
  protected Path getSourcePattern(final Path workingPath, final int index) {
    //output directories are typically of the form
    //out#/part-m-#####/part-m-#####/part-#.trv but we don't want both of those folders because it isn't
    //readable by the TrevniKeySource.
    return new Path(workingPath, PlanningParameters.MULTI_OUTPUT_PREFIX + index + "*/part-*/part-*");
  }

  @Override
  protected Path getDestFile(final Configuration conf, final Path src, final Path dir, final boolean mapOnlyJob) throws IOException {
    Path outputFilename = super.getDestFile(conf, src, dir, true);
    //make sure the dst file is unique in the case there are multiple part-#.trv files per partition.
    return new Path(outputFilename.toString()+"-"+src.getName());
  }

  @Override
  public <T> SourceTarget<T> asSourceTarget(PType<T> ptype) {
    if (ptype instanceof AvroType) {
      return new TrevniKeySourceTarget(path, (AvroType<T>) ptype);
    }
    return null;
  }
}
