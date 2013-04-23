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
package org.apache.crunch.test;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Collection;

import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.hadoop.io.Writable;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;


/**
 * Utilities for integration tests.
 */
public final class Tests {

  private Tests() {
    // nothing
  }

  /**
   * Get the path to and integration test resource file, as per naming convention.
   *
   * @param testCase The executing test case instance
   * @param resourceName The file name of the resource
   * @return The path to the resource (never null)
   * @throws IllegalArgumentException Thrown if the resource doesn't exist
   */
  public static String pathTo(Object testCase, String resourceName) {
    String qualifiedName = resource(testCase, resourceName);
    return Resources.getResource(qualifiedName).getFile();
  }

  /**
   * This doesn't check whether the resource exists!
   *
   * @param testCase
   * @param resourceName
   * @return The path to the resource (never null)
   */
  public static String resource(Object testCase, String resourceName) {
    checkNotNull(testCase);
    checkNotNull(resourceName);

    // Note: We append "Data" because otherwise Eclipse would complain about the
    //       the case's class name clashing with the resource directory's name.
    return testCase.getClass().getName().replaceAll("\\.", "/") + "Data/" + resourceName;
  }

  /**
   * Return our two types of {@link Pipeline}s for a JUnit Parameterized test.
   *
   * @param testCase The executing test case's class
   * @return The collection to return from a {@link Parameters} provider method
   */
  public static Collection<Object[]> pipelinesParams(Class<?> testCase) {
    return ImmutableList.copyOf(
        new Object[][] { { MemPipeline.getInstance() }, { new MRPipeline(testCase) }
    });
  }

  /**
   * Serialize the given Writable into a byte array.
   *
   * @param value The instance to serialize
   * @return The serialized data
   */
  public static byte[] serialize(Writable value) {
    checkNotNull(value);
    try {
      ByteArrayDataOutput out = ByteStreams.newDataOutput();
      value.write(out);
      return out.toByteArray();
    } catch (IOException e) {
      throw new IllegalStateException("cannot serialize", e);
    }
  }

  /**
   * Serialize the src Writable into a byte array, then deserialize it into dest.
   * @param src The instance to serialize
   * @param dest The instance to deserialize into
   * @return dest, for convenience
   */
  public static <T extends Writable> T roundtrip(Writable src, T dest) {
    checkNotNull(src);
    checkNotNull(dest);
    checkArgument(src != dest, "src and dest may not be the same instance");

    try {
      byte[] data = serialize(src);
      dest.readFields(ByteStreams.newDataInput(data));
    } catch (IOException e) {
      throw new IllegalStateException("cannot deserialize", e);
    }
    return dest;
  }
}
