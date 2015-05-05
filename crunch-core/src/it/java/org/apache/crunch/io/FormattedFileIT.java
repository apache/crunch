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
package org.apache.crunch.io;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class FormattedFileIT {

  @Rule
  public TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testReadFormattedFile() throws Exception {
    String urlsFile = tmpDir.copyResourceFileName("urls.txt");
    Pipeline p = new MRPipeline(FormattedFileIT.class, tmpDir.getDefaultConfiguration());
    PTable<LongWritable, Text> urls = p.read(From.formattedFile(urlsFile,
        TextInputFormat.class, LongWritable.class, Text.class));
    List<String> expect = ImmutableList.of("A", "A", "A", "B", "B", "C", "D", "E", "F", "F", "");
    List<String> actual = Lists.newArrayList(Iterables.transform(urls.materialize(),
        new Function<Pair<LongWritable, Text>, String>() {
          @Override
          public String apply(Pair<LongWritable, Text> pair) {
            String str = pair.second().toString();
            if (str.isEmpty()) {
              return str;
            }
            return str.substring(4, 5);
          }
        }));
    assertEquals(expect, actual);
    p.done();
  }
}
