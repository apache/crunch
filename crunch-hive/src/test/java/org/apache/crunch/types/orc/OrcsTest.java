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
package org.apache.crunch.types.orc;

import static org.junit.Assert.*;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.crunch.Pair;
import org.apache.crunch.TupleN;
import org.apache.crunch.io.orc.OrcWritable;
import org.apache.crunch.test.orc.pojos.AddressBook;
import org.apache.crunch.test.orc.pojos.Person;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class OrcsTest {
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  protected static void testInputOutputFn(PType ptype, Object java, OrcWritable orc) {
    initialize(ptype);
    assertEquals(java, ptype.getInputMapFn().map(orc));
    assertEquals(orc, ptype.getOutputMapFn().map(java));
  }
  
  private static void initialize(PType ptype) {
    ptype.getInputMapFn().initialize();
    ptype.getOutputMapFn().initialize();
  }
  
  @Test
  public void testOrcs() {
    String mapValueTypeStr = "struct<a:string,b:int>";
    String typeStr = "struct<a:int,b:string,c:float,d:varchar(64)"
        + ",e:map<string," + mapValueTypeStr + ">>";
    TypeInfo mapValueTypeInfo = TypeInfoUtils.getTypeInfoFromTypeString(mapValueTypeStr);
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeStr);
    PType<OrcStruct> ptype = Orcs.orcs(typeInfo);
    
    HiveVarchar varchar = new HiveVarchar("Hello World", 32);
    Map<Text, OrcStruct> map = new HashMap<Text, OrcStruct>();
    OrcStruct value = OrcUtils.createOrcStruct(mapValueTypeInfo, new Text("age"), new IntWritable(24));
    map.put(new Text("Bob"), value);
    OrcStruct s = OrcUtils.createOrcStruct(typeInfo, new IntWritable(1024), new Text("Alice"),
        null, new HiveVarcharWritable(varchar), map);
    OrcWritable w = new OrcWritable();
    w.set(s);
    
    testInputOutputFn(ptype, s, w);
  }

  @Test
  public void testReflects() {
    PType<AddressBook> ptype = Orcs.reflects(AddressBook.class);
    
    AddressBook ab = new AddressBook();
    ab.setMyName("John Smith");
    ab.setMyNumbers(Arrays.asList("919-333-4452", "650-777-4329"));
    Map<String, Person> contacts = new HashMap<String, Person>();
    contacts.put("Alice", new Person("Alice", 23, Arrays.asList("666-677-9999")));
    contacts.put("Bob", new Person("Bob", 26, Arrays.asList("999-888-1132", "000-222-9934")));
    contacts.put("David", null);
    ab.setContacts(contacts);
    Timestamp now = new Timestamp(System.currentTimeMillis());
    ab.setUpdateTime(now);
    byte[] signature = {0, 0, 64, 68, 39, 0};
    ab.setSignature(signature);
    
    Map<Text, OrcStruct> map = new HashMap<Text, OrcStruct>();
    map.put(new Text("Alice"), OrcUtils.createOrcStruct(Person.TYPE_INFO, new Text("Alice"), new IntWritable(23),
        Arrays.asList(new Text("666-677-9999"))));
    map.put(new Text("Bob"), OrcUtils.createOrcStruct(Person.TYPE_INFO, new Text("Bob"), new IntWritable(26),
        Arrays.asList(new Text("999-888-1132"), new Text("000-222-9934"))));
    map.put(new Text("David"), null);
    OrcStruct s = OrcUtils.createOrcStruct(AddressBook.TYPE_INFO, new Text("John Smith"),
        Arrays.asList(new Text("919-333-4452"), new Text("650-777-4329")), map, new TimestampWritable(now),
        new BytesWritable(signature));
    OrcWritable w = new OrcWritable();
    w.set(s);
    
    testInputOutputFn(ptype, ab, w);
  }
  
  @Test
  public void testTuples() {
    PType<TupleN> ptype = Orcs.tuples(Writables.ints(), Writables.strings(), Orcs.reflects(Person.class),
        Writables.tableOf(Writables.strings(), Orcs.reflects(Person.class)));
    
    TupleN t = new TupleN(1, "John Smith", new Person("Alice", 23, Arrays.asList("666-677-9999")),
        new Pair<String, Person>("Bob", new Person("Bob", 26, Arrays.asList("999-888-1132", "000-222-9934"))));
    
    String typeStr = "struct<a:int,b:string,c:" + Person.TYPE_STR + ",d:struct<d1:string,d2:" + Person.TYPE_STR + ">>";
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeStr);
    String tableTypeStr = "struct<a:string,b:" + Person.TYPE_STR + ">";
    TypeInfo tableTypeInfo = TypeInfoUtils.getTypeInfoFromTypeString(tableTypeStr);
    
    OrcStruct s = OrcUtils.createOrcStruct(typeInfo, new IntWritable(1), new Text("John Smith"),
        OrcUtils.createOrcStruct(Person.TYPE_INFO, new Text("Alice"), new IntWritable(23),
            Arrays.asList(new Text("666-677-9999"))
        ),
        OrcUtils.createOrcStruct(tableTypeInfo, new Text("Bob"),
            OrcUtils.createOrcStruct(Person.TYPE_INFO, new Text("Bob"), new IntWritable(26),
                Arrays.asList(new Text("999-888-1132"), new Text("000-222-9934"))
            )
        )
    );
    OrcWritable w = new OrcWritable();
    w.set(s);
    
    testInputOutputFn(ptype, t, w);
  }

}
