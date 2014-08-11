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
package org.apache.crunch.io.orc;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.TupleN;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.orc.OrcFileSource;
import org.apache.crunch.io.orc.OrcFileTarget;
import org.apache.crunch.io.orc.OrcFileWriter;
import org.apache.crunch.test.orc.pojos.Person;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.orc.OrcUtils;
import org.apache.crunch.types.orc.Orcs;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.collect.Lists;

public class OrcFileSourceTargetIT extends OrcFileTest implements Serializable {
  
  private void generateInputData() throws IOException {
    String typeStr = "struct<name:string,age:int,numbers:array<string>>";
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeStr);
    OrcStruct s = OrcUtils.createOrcStruct(typeInfo, new Text("Alice"), new IntWritable(23),
        Arrays.asList(new Text("919-342-5555"), new Text("650-333-2913")));
    
    OrcFileWriter<OrcStruct> writer = new OrcFileWriter<OrcStruct>(conf, new Path(tempPath, "input.orc"), Orcs.orcs(typeInfo));
    writer.write(s);
    writer.close();
  }
  
  private <T> void testSourceTarget(PType<T> ptype, T expected) {
    Path inputPath = new Path(tempPath, "input.orc");
    Path outputPath = new Path(tempPath, "output");
    
    Pipeline pipeline = new MRPipeline(OrcFileSourceTargetIT.class, conf);
    OrcFileSource<T> source = new OrcFileSource<T>(inputPath, ptype);
    PCollection<T> rows = pipeline.read(source);
    List<T> result = Lists.newArrayList(rows.materialize());
    
    assertEquals(Lists.newArrayList(expected), result);
    
    OrcFileTarget target = new OrcFileTarget(outputPath);
    pipeline.write(rows, target);
    
    assertTrue(pipeline.done().succeeded());
    
    OrcFileReaderFactory<T> reader = new OrcFileReaderFactory<T>(ptype);
    List<T> newResult = Lists.newArrayList(reader.read(fs, inputPath));
    
    assertEquals(Lists.newArrayList(expected), newResult);
  }
  
  @Test
  public void testOrcs() throws IOException {
    generateInputData();
    
    String typeStr = "struct<name:string,age:int,numbers:array<string>>";
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeStr);
    OrcStruct expected = OrcUtils.createOrcStruct(typeInfo, new Text("Alice"), new IntWritable(23),
        Arrays.asList(new Text("919-342-5555"), new Text("650-333-2913")));
    
    testSourceTarget(Orcs.orcs(typeInfo), expected);
  }
  
  @Test
  public void testReflects() throws IOException {
    generateInputData();
    Person expected = new Person("Alice", 23, Arrays.asList("919-342-5555", "650-333-2913"));
    testSourceTarget(Orcs.reflects(Person.class), expected);
  }
  
  @Test
  public void testTuples() throws IOException {
    generateInputData();
    TupleN expected = new TupleN("Alice", 23, Arrays.asList("919-342-5555", "650-333-2913"));
    testSourceTarget(Orcs.tuples(Writables.strings(), Writables.ints(), Writables.collections(Writables.strings())),
        expected);
  }
  
  @Test
  public void testColumnPruning() throws IOException {
    generateInputData();
    
    Pipeline pipeline = new MRPipeline(OrcFileSourceTargetIT.class, conf);
    int[] readColumns = {0, 1};
    OrcFileSource<Person> source = new OrcFileSource<Person>(new Path(tempPath, "input.orc"),
        Orcs.reflects(Person.class), readColumns);
    PCollection<Person> rows = pipeline.read(source);
    List<Person> result = Lists.newArrayList(rows.materialize());
    
    Person expected = new Person("Alice", 23, null);
    assertEquals(Lists.newArrayList(expected), result);
  }
  
  @Test
  public void testGrouping() throws IOException {
    String typeStr = "struct<name:string,age:int,numbers:array<string>>";
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeStr);
    OrcStruct s1 = OrcUtils.createOrcStruct(typeInfo, new Text("Bob"), new IntWritable(28), null);
    OrcStruct s2 = OrcUtils.createOrcStruct(typeInfo, new Text("Bob"), new IntWritable(28), null);
    OrcStruct s3 = OrcUtils.createOrcStruct(typeInfo, new Text("Alice"), new IntWritable(23),
        Arrays.asList(new Text("444-333-9999")));
    OrcStruct s4 = OrcUtils.createOrcStruct(typeInfo, new Text("Alice"), new IntWritable(36),
        Arrays.asList(new Text("919-342-5555"), new Text("650-333-2913")));

    Path inputPath = new Path(tempPath, "input.orc");
    OrcFileWriter<OrcStruct> writer = new OrcFileWriter<OrcStruct>(conf, inputPath, Orcs.orcs(typeInfo));
    writer.write(s1);
    writer.write(s2);
    writer.write(s3);
    writer.write(s4);
    writer.close();
    
    Pipeline pipeline = new MRPipeline(OrcFileSourceTargetIT.class, conf);
    OrcFileSource<Person> source = new OrcFileSource<Person>(inputPath, Orcs.reflects(Person.class));
    PCollection<Person> rows = pipeline.read(source);
    PTable<Person, Long> count = rows.count();

    List<Pair<Person, Long>> result = Lists.newArrayList(count.materialize());
    List<Pair<Person, Long>> expected = Lists.newArrayList(
        Pair.of(new Person("Alice", 23, Arrays.asList("444-333-9999")), 1L),
        Pair.of(new Person("Alice", 36, Arrays.asList("919-342-5555", "650-333-2913")), 1L),
        Pair.of(new Person("Bob", 28, null), 2L));
    
    assertEquals(expected, result);
  }

}
