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
package org.apache.crunch.lib;

import com.google.common.base.Strings;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import org.apache.avro.util.Utf8;
import org.apache.crunch.DoFn;
import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.emit.InMemoryEmitter;
import org.apache.crunch.test.Employee;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;

public class DoFnsTest {

  private static class AvroIterable implements Iterable<Employee> {
    @Override
    public Iterator<Employee> iterator() {
      final Employee rec = new Employee(new Utf8("something"), 10000, new Utf8(""));
      return new AbstractIterator<Employee>() {
        private int n = 0;
        @Override
        protected Employee computeNext() {
          n++;
          if (n > 3) return endOfData();
          rec.setDepartment(new Utf8(Strings.repeat("*", n)));
          return rec;
        }
      };
    }
  }

  private static class CollectingMapFn extends
          MapFn<Pair<String, Iterable<Employee>>, Collection<Employee>> {

    @Override
    public Collection<Employee> map(Pair<String, Iterable<Employee>> input) {
      return Lists.newArrayList(input.second());
    }
  }

  @Test
  public void testDetach() {
    Collection<Employee> expected = Lists.newArrayList(
            new Employee(new Utf8("something"), 10000, new Utf8("*")),
            new Employee(new Utf8("something"), 10000, new Utf8("**")),
            new Employee(new Utf8("something"), 10000, new Utf8("***"))
    );
    DoFn<Pair<String, Iterable<Employee>>, Collection<Employee>> doFn =
            DoFns.detach(new CollectingMapFn(), Avros.specifics(Employee.class));
    Pair<String, Iterable<Employee>> input = Pair.of("key", (Iterable<Employee>) new AvroIterable());
    InMemoryEmitter<Collection<Employee>> emitter = new InMemoryEmitter<Collection<Employee>>();

    doFn.configure(new Configuration());
    doFn.initialize();
    doFn.process(input, emitter);
    doFn.cleanup(emitter);

    assertEquals(expected, emitter.getOutput().get(0));
  }

}