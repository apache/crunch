/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
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
package com.cloudera.crunch.lib.join;

import static com.cloudera.crunch.types.avro.Avros.*;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.io.From;
import com.cloudera.crunch.test.Employee;
import com.cloudera.crunch.test.Person;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class MultiAvroSchemaJoinTest {

  private File personFile;
  private File employeeFile;
  
  @Before
  public void setUp() throws Exception {
    this.personFile = File.createTempFile("person", ".avro");
    this.employeeFile = File.createTempFile("employee", ".avro");

    DatumWriter<Person> pdw = new SpecificDatumWriter<Person>();
    DataFileWriter<Person> pfw = new DataFileWriter<Person>(pdw);
    pfw.create(Person.SCHEMA$, personFile);
    Person p1 = new Person();
    p1.setName("Josh");
    p1.setAge(19);
    p1.setSiblingnames(ImmutableList.<CharSequence>of("Kate", "Mike"));
    pfw.append(p1);
    Person p2 = new Person();
    p2.setName("Kate");
    p2.setAge(17);
    p2.setSiblingnames(ImmutableList.<CharSequence>of("Josh", "Mike"));
    pfw.append(p2);
    Person p3 = new Person();
    p3.setName("Mike");
    p3.setAge(12);
    p3.setSiblingnames(ImmutableList.<CharSequence>of("Josh", "Kate"));
    pfw.append(p3);
    pfw.close();
    
    DatumWriter<Employee> edw = new SpecificDatumWriter<Employee>();
    DataFileWriter<Employee> efw = new DataFileWriter<Employee>(edw);
    efw.create(Employee.SCHEMA$, employeeFile);
    Employee e1 = new Employee();
    e1.setName("Kate");
    e1.setSalary(100000);
    e1.setDepartment("Marketing");
    efw.append(e1);
    efw.close();
  }
  
  @After
  public void tearDown() throws Exception {
    personFile.delete();
    employeeFile.delete();
  }
  
  public static class NameFn<K extends SpecificRecord> extends MapFn<K, String> {
    @Override
    public String map(K input) {
      Schema s = input.getSchema();
      Schema.Field f = s.getField("name");
      return input.get(f.pos()).toString();
    }
  }
  
  @Test
  public void testJoin() throws Exception {
     Pipeline p = new MRPipeline(MultiAvroSchemaJoinTest.class);
     PCollection<Person> people = p.read(From.avroFile(personFile.getAbsolutePath(), records(Person.class)));
     PCollection<Employee> employees = p.read(From.avroFile(employeeFile.getAbsolutePath(), records(Employee.class)));
     
     Iterable<Pair<Person, Employee>> result = people.by(new NameFn<Person>(), strings())
         .join(employees.by(new NameFn<Employee>(), strings())).values().materialize();
     List<Pair<Person, Employee>> v = Lists.newArrayList(result);
     assertEquals(1, v.size());
     assertEquals("Kate", v.get(0).first().getName().toString());
     assertEquals("Kate", v.get(0).second().getName().toString());
  }
}
