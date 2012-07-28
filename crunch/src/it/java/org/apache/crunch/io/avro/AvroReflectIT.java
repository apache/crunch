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
package org.apache.crunch.io.avro;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.crunch.types.avro.Avros;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;

public class AvroReflectIT implements Serializable {

  static class StringWrapper {
    private String value;

    public StringWrapper() {
      this(null);
    }

    public StringWrapper(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return String.format("<StringWrapper(%s)>", value);
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((value == null) ? 0 : value.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      StringWrapper other = (StringWrapper) obj;
      if (value == null) {
        if (other.value != null)
          return false;
      } else if (!value.equals(other.value))
        return false;
      return true;
    }

  }
  @Rule
  public transient TemporaryPath tmpDir = TemporaryPaths.create();

  @Test
  public void testReflection() throws IOException {
    Pipeline pipeline = new MRPipeline(AvroReflectIT.class, tmpDir.getDefaultConfiguration());
    PCollection<StringWrapper> stringWrapperCollection = pipeline.readTextFile(tmpDir.copyResourceFileName("set1.txt"))
        .parallelDo(new MapFn<String, StringWrapper>() {

          @Override
          public StringWrapper map(String input) {
            StringWrapper stringWrapper = new StringWrapper();
            stringWrapper.setValue(input);
            return stringWrapper;
          }
        }, Avros.reflects(StringWrapper.class));

    List<StringWrapper> stringWrappers = Lists.newArrayList(stringWrapperCollection.materialize());

    pipeline.done();

    assertEquals(Lists.newArrayList(new StringWrapper("b"), new StringWrapper("c"), new StringWrapper("a"),
        new StringWrapper("e")), stringWrappers);

  }

}
