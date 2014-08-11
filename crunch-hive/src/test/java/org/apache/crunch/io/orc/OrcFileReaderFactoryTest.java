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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.List;

import org.apache.crunch.types.PType;
import org.apache.crunch.types.orc.OrcUtils;
import org.apache.crunch.types.orc.Orcs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class OrcFileReaderFactoryTest extends OrcFileTest {
  
  @Test
  public void testColumnPruning() throws IOException {
    Path path = new Path(tempPath, "test.orc");
    
    String typeStr = "struct<a:int,b:string,c:float>";
    TypeInfo info = TypeInfoUtils.getTypeInfoFromTypeString(typeStr);
    StructObjectInspector soi = (StructObjectInspector) OrcStruct.createObjectInspector(info);
    PType<OrcStruct> ptype = Orcs.orcs(info);
    
    OrcFileWriter<OrcStruct> writer = new OrcFileWriter<OrcStruct>(conf, path, ptype);
    writer.write(OrcUtils.createOrcStruct(info, new IntWritable(1), new Text("Alice"), new FloatWritable(167.2f)));
    writer.write(OrcUtils.createOrcStruct(info, new IntWritable(2), new Text("Bob"), new FloatWritable(179.7f)));
    writer.close();
    
    int[] readColumns = {1};
    OrcFileSource<OrcStruct> source = new OrcFileSource<OrcStruct>(path, ptype, readColumns);
    for (OrcStruct row : source.read(conf)) {
      List<Object> list = soi.getStructFieldsDataAsList(row);
      assertNull(list.get(0));
      assertNotNull(list.get(1));
      assertNull(list.get(2));
    }
  }

}
