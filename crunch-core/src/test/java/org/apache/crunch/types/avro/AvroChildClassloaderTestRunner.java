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
package org.apache.crunch.types.avro;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;

import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

public class AvroChildClassloaderTestRunner extends BlockJUnit4ClassRunner {

  public AvroChildClassloaderTestRunner(Class<?> clazz) throws InitializationError {
    super(getFromTestClassloader(clazz));
  }

  private static Class<?> getFromTestClassloader(Class<?> clazz) throws InitializationError {
    try {
      ClassLoader testClassLoader = new TestClassLoader();
      return Class.forName(clazz.getName(), true, testClassLoader);
    } catch (ClassNotFoundException e) {
      throw new InitializationError(e);
    }
  }

  public static class TestClassLoader extends URLClassLoader {

    private static ClassLoader parentClassLoader;
    private static URL[] crunchURLs;
    static {
      ClassLoader classLoader = getSystemClassLoader();
      URL[] urls = null;
      if (classLoader instanceof URLClassLoader) {
	 urls = ((URLClassLoader) classLoader).getURLs();
      } else {
        String[] pieces = ManagementFactory.getRuntimeMXBean().getClassPath().split(File.pathSeparator);
        urls = new URL[pieces.length];
        for (int i = 0; i < pieces.length; i++) {
          try {
            urls[i] = new File(pieces[i]).toURI().toURL();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }

      Collection<URL> crunchURLs = new ArrayList<URL>();
      Collection<URL> otherURLs = new ArrayList<URL>();
      for (URL url : urls) {
        if (url.getPath().matches("^.*/crunch-?.*/.*$")) {
          crunchURLs.add(url);
        } else {
          otherURLs.add(url);
        }
      }

      TestClassLoader.crunchURLs = crunchURLs.toArray(new URL[crunchURLs.size()]);
      parentClassLoader = new URLClassLoader(otherURLs.toArray(new URL[otherURLs.size()]), classLoader);
    }

    public TestClassLoader() {
      super(crunchURLs, parentClassLoader);
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
      if (name.startsWith("org.junit")) {
        return getSystemClassLoader().loadClass(name);
      }

      return super.loadClass(name);
    }
  }
}
