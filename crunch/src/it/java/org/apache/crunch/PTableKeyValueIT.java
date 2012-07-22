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
package org.apache.crunch;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import junit.framework.Assert;

import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.At;
import org.apache.crunch.test.FileHelper;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;

@RunWith(value = Parameterized.class)
public class PTableKeyValueIT implements Serializable {

  private static final long serialVersionUID = 4374227704751746689L;

  private transient PTypeFamily typeFamily;
  private transient MRPipeline pipeline;
  private transient String inputFile;
  @Rule
  public transient TemporaryPath temporaryPath = new TemporaryPath();

  @Before
  public void setUp() throws IOException {
    pipeline = new MRPipeline(PTableKeyValueIT.class, temporaryPath.setTempLoc(new Configuration()));
    inputFile = FileHelper.createTempCopyOf("set1.txt");
  }

  @After
  public void tearDown() {
    pipeline.done();
  }

  public PTableKeyValueIT(PTypeFamily typeFamily) {
    this.typeFamily = typeFamily;
  }

  @Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] { { WritableTypeFamily.getInstance() }, { AvroTypeFamily.getInstance() } };
    return Arrays.asList(data);
  }

  @Test
  public void testKeysAndValues() throws Exception {

    PCollection<String> collection = pipeline.read(At.textFile(inputFile, typeFamily.strings()));

    PTable<String, String> table = collection.parallelDo(new DoFn<String, Pair<String, String>>() {

      @Override
      public void process(String input, Emitter<Pair<String, String>> emitter) {
        emitter.emit(Pair.of(input.toUpperCase(), input));

      }
    }, typeFamily.tableOf(typeFamily.strings(), typeFamily.strings()));

    PCollection<String> keys = table.keys();
    PCollection<String> values = table.values();

    ArrayList<String> keyList = Lists.newArrayList(keys.materialize().iterator());
    ArrayList<String> valueList = Lists.newArrayList(values.materialize().iterator());

    Assert.assertEquals(keyList.size(), valueList.size());
    for (int i = 0; i < keyList.size(); i++) {
      Assert.assertEquals(keyList.get(i), valueList.get(i).toUpperCase());
    }
  }

}
