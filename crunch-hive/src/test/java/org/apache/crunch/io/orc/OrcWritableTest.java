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

import java.util.List;

import org.apache.crunch.types.orc.OrcUtils;
import org.apache.crunch.types.writable.WritableDeepCopier;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class OrcWritableTest {
  
  @Test
  public void testDeepCopy() {
    String typeStr = "struct<a:int,b:string,c:float>";
    TypeInfo info = TypeInfoUtils.getTypeInfoFromTypeString(typeStr);
    StructObjectInspector oi = (StructObjectInspector) OrcStruct.createObjectInspector(info);
    BinarySortableSerDe serde = OrcUtils.createBinarySerde(info);
    
    OrcStruct struct = OrcUtils.createOrcStruct(info,
        new IntWritable(1), new Text("Alice"), new FloatWritable(165.3f));
    OrcWritable writable = new OrcWritable();
    writable.set(struct);
    assertTrue(struct == writable.get());
    
    writable.setObjectInspector(oi);
    writable.setSerde(serde);
    
    WritableDeepCopier<OrcWritable> deepCopier = new WritableDeepCopier<OrcWritable>(OrcWritable.class);
    OrcWritable copied = deepCopier.deepCopy(writable);
    assertTrue(writable != copied);
    assertEquals(writable, copied);
    
    copied.setObjectInspector(oi);
    copied.setSerde(serde);
    OrcStruct copiedStruct = copied.get();
    assertTrue(struct != copiedStruct);
    assertEquals(struct, copiedStruct);
    
    List<Object> items = oi.getStructFieldsDataAsList(struct);
    List<Object> copiedItems = oi.getStructFieldsDataAsList(copiedStruct);
    for (int i = 0; i < items.size(); i++) {
      assertTrue(items.get(i) != copiedItems.get(i));
      assertEquals(items.get(i), copiedItems.get(i));
    }
    
    OrcWritable copied2 = deepCopier.deepCopy(copied);
    assertTrue(copied2 != copied);
    assertEquals(copied2, copied);
    
    copied2.setObjectInspector(oi);
    copied2.setSerde(serde);
    OrcStruct copiedStruct2 = copied2.get();
    assertTrue(copiedStruct2 != copiedStruct);
    assertEquals(copiedStruct2, copiedStruct);
    
    List<Object> copiedItems2 = oi.getStructFieldsDataAsList(copiedStruct2);
    for (int i = 0; i < items.size(); i++) {
      assertTrue(copiedItems2.get(i) != copiedItems.get(i));
      assertEquals(copiedItems2.get(i), copiedItems.get(i));
    }
  }
  
  @Test
  public void testCompareTo() {
    String typeStr = "struct<a:int,b:string,c:float>";
    TypeInfo info = TypeInfoUtils.getTypeInfoFromTypeString(typeStr);
    StructObjectInspector oi = (StructObjectInspector) OrcStruct.createObjectInspector(info);
    BinarySortableSerDe serde = OrcUtils.createBinarySerde(info);
    
    OrcStruct struct1 = OrcUtils.createOrcStruct(info, new IntWritable(1), new Text("AAA"), new FloatWritable(3.2f));
    OrcStruct struct2 = OrcUtils.createOrcStruct(info, new IntWritable(1), new Text("AAB"), null);
    OrcStruct struct3 = OrcUtils.createOrcStruct(info, new IntWritable(2), new Text("AAA"), null);
    OrcStruct struct4 = OrcUtils.createOrcStruct(info, new IntWritable(2), new Text("AAA"), new FloatWritable(3.2f));
    
    OrcWritable writable1 = new OrcWritable();
    writable1.set(struct1);
    OrcWritable writable2 = new OrcWritable();
    writable2.set(struct2);
    OrcWritable writable3 = new OrcWritable();
    writable3.set(struct3);
    OrcWritable writable4 = new OrcWritable();
    writable4.set(struct4);
    
    writable1.setObjectInspector(oi);
    writable2.setObjectInspector(oi);
    writable3.setObjectInspector(oi);
    writable4.setObjectInspector(oi);
    writable1.setSerde(serde);
    writable2.setSerde(serde);
    writable3.setSerde(serde);
    writable4.setSerde(serde);
    
    assertTrue(writable1.compareTo(writable2) < 0);
    assertTrue(writable2.compareTo(writable3) < 0);
    assertTrue(writable1.compareTo(writable3) < 0);
    assertTrue(writable3.compareTo(writable4) < 0);
  }

}
