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
package org.apache.crunch.lib.join;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public class BloomFilterLeftOuterJoinIT extends AbstractLeftOuterJoinIT {

  private static String saveTempDir;
  
  @BeforeClass
  public static void setUpClass(){
    
    // Ensure a consistent temporary directory for use of the DistributedCache.
    
    // The DistributedCache technically isn't supported when running in local mode, and the default
    // temporary directiory "/tmp" is used as its location. This typically only causes an issue when 
    // running integration tests on Mac OS X, as OS X doesn't use "/tmp" as it's default temporary
    // directory. The following call ensures that "/tmp" is used as the temporary directory on all platforms.
    saveTempDir = System.setProperty("java.io.tmpdir", "/tmp");
  }
  
  @AfterClass
  public static void tearDownClass(){
    System.setProperty("java.io.tmpdir", saveTempDir);
  }
  
  @Override
  protected <K, U, V> JoinStrategy<K, U, V> getJoinStrategy() {
    return new BloomFilterJoinStrategy<K, U, V>(20000);
  }

}
