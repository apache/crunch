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
package org.apache.crunch.io.hcatalog;

import org.apache.crunch.test.TemporaryPath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

/**
 * Test suite to re-use the same hive metastore instance for all tests in the
 * suite
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({ HCatSourceITSpec.class, HCatTargetITSpec.class })
public class HCatTestSuiteIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(HCatTestSuiteIT.class);
  private static boolean runAsSuite = false;

  public static TemporaryPath hadoopTempDir = new TemporaryPath("crunch.tmp.dir", "hadoop.tmp.dir");

  static HiveConf hconf;
  static IMetaStoreClient client;
  static Configuration conf = null;

  @BeforeClass
  public static void startSuite() throws Exception {
    runAsSuite = true;
    setupFileSystem();
    setupMetaStore();
  }

  @AfterClass
  public static void endSuite() throws Exception {
    cleanup();
  }

  public static Configuration getConf() {
    return conf;
  }

  public static TemporaryPath getRootPath() {
    return hadoopTempDir;
  }

  public static IMetaStoreClient getClient() {
    return client;
  }

  private static void setupMetaStore() throws Exception {
    conf = hadoopTempDir.getDefaultConfiguration();
    // set the warehouse location to the location of the temp dir, so managed
    // tables return a size estimate of the table
    String databaseLocation = hadoopTempDir.getPath("metastore_db").toString();
    String derbyLocation = hadoopTempDir.getPath("derby.log").toString();
    String jdbcUrl = "jdbc:derby:;databaseName=" + databaseLocation + ";create=true";
    conf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, jdbcUrl);
    conf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.toString(), hadoopTempDir.getRootPath().toString());
    // allow HMS to create any tables necessary
    conf.set("datanucleus.schema.autoCreateTables", "true");
    // disable verification as the tables won't exist at startup
    conf.set("hive.metastore.schema.verification", "false");
    // write derby logs to the temp directory to be cleaned up automagically after the test runs
    System.setProperty("derby.stream.error.file", derbyLocation);
    hconf = HCatUtil.getHiveConf(conf);
    client = HCatUtil.getHiveMetastoreClient(hconf);
  }

  private static void setupFileSystem() throws Exception {
    try {
      hadoopTempDir.create();
    } catch (Throwable throwable) {
      throw (Exception) throwable;
    }
  }

  public static void startTest() throws Exception {
    if (!runAsSuite) {
      setupFileSystem();
      setupMetaStore();
    }
  }

  public static void endTest() throws Exception {
    if (!runAsSuite) {
      cleanup();
    }
  }

  private static void cleanup() throws IOException {
    hadoopTempDir.delete();
    client.close();
  }
}
