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
import java.util.Arrays;
import java.util.Iterator;

import org.apache.crunch.io.orc.OrcFileSource;
import org.apache.crunch.io.orc.OrcFileWriter;
import org.apache.crunch.test.orc.pojos.Person;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.orc.Orcs;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class OrcFileReaderWriterTest extends OrcFileTest {
  
  @Test
  public void testReadWrite() throws IOException {
    Path path = new Path(tempPath, "test.orc");
    PType<Person> ptype = Orcs.reflects(Person.class);
    OrcFileWriter<Person> writer = new OrcFileWriter<Person>(conf, path, ptype);
    
    Person p1 = new Person("Alice", 23, Arrays.asList("666-677-9999"));
    Person p2 = new Person("Bob", 26, null);
    
    writer.write(p1);
    writer.write(p2);
    writer.close();
    
    OrcFileSource<Person> reader = new OrcFileSource<Person>(path, ptype);
    Iterator<Person> iter = reader.read(conf).iterator();
    assertEquals(p1, iter.next());
    assertEquals(p2, iter.next());
  }

}
