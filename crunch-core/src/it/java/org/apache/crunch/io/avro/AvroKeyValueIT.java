/*
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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.crunch.io.avro;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.Serializable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.test.CrunchTestSupport;
import org.apache.crunch.test.Person;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.avro.ReflectedPerson;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

/**
 * Tests for verifying behavior with Avro produced using the org.apache.avro.mapred.*
 * and org.apache.avro.mapreduce.* APIs.
 */
public class AvroKeyValueIT extends CrunchTestSupport implements Serializable {

  @Test
  public void testInputFromMapReduceKeyValueFile_Generic() throws InterruptedException, IOException, ClassNotFoundException {

    Path keyValuePath = produceMapReduceOutputFile();

    Pipeline pipeline = new MRPipeline(AvroKeyValueIT.class, tempDir.getDefaultConfiguration());
    PTable<Person, Integer> personTable = pipeline.read(
        From.avroTableFile(keyValuePath, Avros.tableOf(Avros.specifics(Person.class), Avros.ints())));

    org.apache.crunch.Pair<Person, Integer> firstEntry = Iterables.getFirst(personTable.materialize(), null);

    assertEquals("a", firstEntry.first().getName().toString());
    assertEquals(Integer.valueOf(1), firstEntry.second());

    pipeline.done();

  }

  @Test
  public void testInputFromMapRedKeyValueFile_Specific() throws IOException {
    Path keyValuePath = produceMapRedOutputFile();

    Pipeline pipeline = new MRPipeline(AvroKeyValueIT.class, tempDir.getDefaultConfiguration());
    PTable<Person, Integer> personTable = pipeline.read(
        From.avroTableFile(keyValuePath, Avros.keyValueTableOf(Avros.specifics(Person.class), Avros.ints())));

    org.apache.crunch.Pair<Person, Integer> firstEntry = Iterables.getFirst(personTable.materialize(), null);

    assertEquals("a", firstEntry.first().getName().toString());
    assertEquals(Integer.valueOf(1), firstEntry.second());

    // Verify that deep copying on this PType works as well
    PTableType<Person, Integer> tableType = Avros.keyValueTableOf(Avros.specifics(Person.class), Avros.ints());
    tableType.initialize(tempDir.getDefaultConfiguration());
    org.apache.crunch.Pair<Person, Integer> detachedPair = tableType.getDetachedValue(firstEntry);
    assertEquals(firstEntry, detachedPair);

    pipeline.done();
  }

  @Test
  public void testInputFromMapRedKeyValueFile_Reflect() throws IOException {
    Path keyValuePath = produceMapRedOutputFile();

    Pipeline pipeline = new MRPipeline(AvroKeyValueIT.class, tempDir.getDefaultConfiguration());
    PTable<ReflectedPerson, Integer> personTable = pipeline.read(
        From.avroTableFile(keyValuePath, Avros.keyValueTableOf(Avros.reflects(ReflectedPerson.class), Avros.ints())));

    org.apache.crunch.Pair<ReflectedPerson, Integer> firstEntry = Iterables.getFirst(personTable.materialize(), null);

    assertEquals("a", firstEntry.first().getName().toString());
    assertEquals(Integer.valueOf(1), firstEntry.second());

    // Verify that deep copying on this PType works as well
    PTableType<ReflectedPerson, Integer> tableType =
        Avros.keyValueTableOf(Avros.reflects(ReflectedPerson.class), Avros.ints());
    tableType.initialize(tempDir.getDefaultConfiguration());
    org.apache.crunch.Pair<ReflectedPerson, Integer> detachedPair = tableType.getDetachedValue(firstEntry);
    assertEquals(firstEntry, detachedPair);

    pipeline.done();
  }

  /**
   * Produces an Avro file using the org.apache.avro.mapred.* API.
   */
  private Path produceMapRedOutputFile() throws IOException {

    JobConf conf = new JobConf(tempDir.getDefaultConfiguration(), AvroKeyValueIT.class);

    org.apache.avro.mapred.AvroJob.setOutputSchema(
        conf,
        Pair.getPairSchema(Person.SCHEMA$, Schema.create(Schema.Type.INT)));


    conf.setMapperClass(MapRedPersonMapper.class);
    conf.setNumReduceTasks(0);

    conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);



    Path outputPath = new Path(tempDir.getFileName("mapreduce_output"));
    org.apache.hadoop.mapred.FileInputFormat.setInputPaths(conf, tempDir.copyResourcePath("letters.txt"));
    org.apache.hadoop.mapred.FileOutputFormat.setOutputPath(conf, outputPath);

    RunningJob runningJob = JobClient.runJob(conf);
    runningJob.waitForCompletion();

    return outputPath;

  }

  /**
   * Produces an Avro file using the org.apache.avro.mapreduce.* API.
   */
  private Path produceMapReduceOutputFile() throws IOException, ClassNotFoundException, InterruptedException {


    Job job = new Job(tempDir.getDefaultConfiguration());
    job.setJarByClass(AvroKeyValueIT.class);
    job.setJobName("Color Count");

    Path outputPath = new Path(tempDir.getFileName("mapreduce_output"));

    FileInputFormat.setInputPaths(job, tempDir.copyResourcePath("letters.txt"));
    FileOutputFormat.setOutputPath(job, outputPath);

    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(MapReducePersonMapper.class);
    job.setNumReduceTasks(0);
    AvroJob.setOutputKeySchema(job, Person.SCHEMA$);
    AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.INT));

    job.setOutputFormatClass(AvroKeyValueOutputFormat.class);

    boolean success = job.waitForCompletion(true);

    if (!success) {
      throw new RuntimeException("Job failed");
    }

    return outputPath;
  }

  public static class MapReducePersonMapper extends
      Mapper<LongWritable, Text, AvroKey<Person>, AvroValue<Integer>> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      Person person = Person.newBuilder()
          .setName(value.toString())
          .setAge(value.toString().length())
          .setSiblingnames(ImmutableList.<CharSequence>of())
          .build();
      context.write(
          new AvroKey<Person>(person),
          new AvroValue<Integer>(1));

    }
  }

  public static class MapRedPersonMapper implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, AvroWrapper<Pair<Person,Integer>>, NullWritable> {
    @Override
    public void map(LongWritable key, Text value, OutputCollector<AvroWrapper<Pair<Person,Integer>>, NullWritable> outputCollector, Reporter reporter) throws IOException {
      Person person = Person.newBuilder()
          .setName(value.toString())
          .setAge(value.toString().length())
          .setSiblingnames(ImmutableList.<CharSequence>of())
          .build();
      outputCollector.collect(
          new AvroWrapper<Pair<Person, Integer>>(new Pair<Person, Integer>(person, 1)),
          NullWritable.get());
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void configure(JobConf entries) {
    }
  }

}
