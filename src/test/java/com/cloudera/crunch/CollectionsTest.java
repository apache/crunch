/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
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
package com.cloudera.crunch;

import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.newInputStreamSupplier;
import static org.junit.Assert.assertTrue;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.type.PTypeFamily;
import com.cloudera.crunch.type.avro.AvroTypeFamily;
import com.cloudera.crunch.type.writable.WritableTypeFamily;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.junit.Test;

@SuppressWarnings("serial")
public class CollectionsTest {
  
  public static class AggregateStringListFn implements CombineFn.Aggregator<Collection<String>> {
    private final Collection<String> rtn = Lists.newArrayList();
    
    @Override
    public void reset() {
      rtn.clear();
    }
    
    @Override
    public void update(Collection<String> values) {
      rtn.addAll(values);
    }      
    
    @Override
    public Iterable<Collection<String>> results() {
      return ImmutableList.of(rtn);
    }
  }
  
  public static PTable<String, Collection<String>> listOfCharcters(PCollection<String> lines, PTypeFamily typeFamily) {
     
    return lines.parallelDo(new DoFn<String, Pair<String, Collection<String>>>() {
      @Override
      public void process(String line, Emitter<Pair<String, Collection<String>>> emitter) {
        for (String word : line.split("\\s+")) {
          Collection<String> characters = Lists.newArrayList();
          for(char c : word.toCharArray()) {
            characters.add(String.valueOf(c));
          }
          emitter.emit(Pair.of(word, characters));
        }
      }
    }, typeFamily.tableOf(typeFamily.strings(), typeFamily.collections(typeFamily.strings())))
    .groupByKey()
    .combineValues(CombineFn.<String, Collection<String>>aggregator(new AggregateStringListFn()));
  }
  
  @Test
  public void testWritables() throws IOException {
    run(new MRPipeline(CollectionsTest.class), WritableTypeFamily.getInstance());
  }

  @Test
  public void testAvro() throws IOException {
    run(new MRPipeline(CollectionsTest.class), AvroTypeFamily.getInstance());
  }
  
  public void run(Pipeline pipeline, PTypeFamily typeFamily) throws IOException {
    File input = File.createTempFile("shakes", "txt");
    input.deleteOnExit();
    Files.copy(newInputStreamSupplier(getResource("shakes.txt")), input);
    
    PCollection<String> shakespeare = pipeline.readTextFile(input.getAbsolutePath());
    Iterable<Pair<String, Collection<String>>> lines = listOfCharcters(shakespeare, typeFamily).materialize();
    
    boolean passed = false;
    for (Pair<String, Collection<String>> line : lines) {
      if(line.first().startsWith("yellow")) {
        passed = true;
        break;
      }
    }
    pipeline.done();
    assertTrue(passed);
  }  
}
