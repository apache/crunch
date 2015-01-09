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
package org.apache.crunch.impl.spark;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Field.Order;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.crunch.impl.spark.serde.AvroSerDe;
import org.apache.crunch.types.avro.Avros;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class AvroByteArrayTest {
  @Test
  public void fieldsWithIgnoredSortOrderAreNotUsedInEquals() throws Exception {
    Schema mySchema = Schema.createRecord("foo", "", "", false);
    mySchema.setFields(Lists.newArrayList(new Field("field1",
        Schema.create(Type.STRING),
        null,
        JsonNodeFactory.instance.textNode(""),
        Order.ASCENDING), new Field("field2",
        Schema.create(Type.STRING),
        null,
        JsonNodeFactory.instance.textNode(""),
        Order.IGNORE)));

    GenericRecordBuilder myGRB = new GenericRecordBuilder(mySchema);
    Record myRecord1 = myGRB.set("field1", "hello").set("field2", "world").build();
    Record myRecord2 = myGRB.set("field1", "hello").set("field2", "there").build();
    assertEquals(myRecord1, myRecord2);

    AvroSerDe serde = new AvroSerDe(Avros.generics(mySchema), null);
    assertEquals(serde.toBytes(myRecord1), serde.toBytes(myRecord2));
  }
}
