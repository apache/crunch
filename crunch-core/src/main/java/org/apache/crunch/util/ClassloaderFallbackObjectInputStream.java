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
package org.apache.crunch.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

/**
 * A custom {@link ObjectInputStream} that falls back to the thread context classloader
 * if the class can't be found with the usual classloader that {@link
 * ObjectInputStream} uses. This is needed when running in the Scala REPL.
 * See https://issues.scala-lang.org/browse/SI-2403.
 */
public class ClassloaderFallbackObjectInputStream extends ObjectInputStream {
  public ClassloaderFallbackObjectInputStream(InputStream in) throws IOException {
    super(in);
  }

  @Override
  protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException,
      ClassNotFoundException {
    try {
      return super.resolveClass(desc);
    } catch (ClassNotFoundException e) {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      return Class.forName(desc.getName(), false, cl);
    }
  }
}
