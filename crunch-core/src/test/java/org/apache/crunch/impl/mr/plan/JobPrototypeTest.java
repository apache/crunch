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
package org.apache.crunch.impl.mr.plan;

import com.google.common.collect.HashMultimap;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mr.collect.PGroupedTableImpl;
import org.apache.crunch.io.impl.FileTargetImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

@RunWith(MockitoJUnitRunner.class)
public class JobPrototypeTest {

  public static final String DFS_REPLICATION = "dfs.replication";
  public static final String TEST_INITIAL_DFS_REPLICATION = "42";
  public static final String TEST_TMP_DIR_REPLICATION = "1";

  @Mock private Path mockPath;
  @Mock private FileTargetImpl mockTarget;
  @Mock private FileSystem mockFs;
  @Mock private DoNode mockNode;
  @Mock private PGroupedTableImpl<String, String> mockPgroup;
  @Mock private Set<NodePath> mockInputs;
  private JobPrototype jobPrototypeUnderTest;
  private Configuration testConfiguration = new Configuration();

  @Before
  public void setUp() {
    testConfiguration.set("dfs.replication.initial", TEST_INITIAL_DFS_REPLICATION);
    testConfiguration.set("crunch.tmp.dir.replication", TEST_TMP_DIR_REPLICATION);
    doReturn(new Object[]{}).when(mockInputs).toArray();
    jobPrototypeUnderTest=  JobPrototype.createMapReduceJob(42,
            mockPgroup, mockInputs, mockPath);
  }

  @Test
  public void initialReplicationFactorSetForLeafOutputTargets() {
    jobPrototypeUnderTest.setJobReplication(testConfiguration, true);

    assertEquals(TEST_TMP_DIR_REPLICATION, testConfiguration.get(DFS_REPLICATION));
  }

  @Test
  public void userDefinedTmpDirReplicationFactorSetForIntermediateTargets() {
    jobPrototypeUnderTest.setJobReplication(testConfiguration, false);

    assertEquals(TEST_INITIAL_DFS_REPLICATION, testConfiguration.get(DFS_REPLICATION));
  }

  @Test
  public void initialReplicationFactorSetIfUserSpecified() {
    jobPrototypeUnderTest.handleInitialReplication(testConfiguration);

    assertEquals(TEST_INITIAL_DFS_REPLICATION, testConfiguration.get("dfs.replication.initial"));
  }

  @Test
  public void initialReplicationFactorUsedFromFileSystem() throws IOException {
    testConfiguration = new Configuration();
    HashMultimap<Target, NodePath> targetNodePaths = HashMultimap.create();
    targetNodePaths.put(mockTarget, new NodePath());
    doReturn(mockPath).when(mockTarget).getPath();
    doReturn(mockFs).when(mockPath).getFileSystem(any(Configuration.class));
    Configuration c = new Configuration();
    c.set("dfs.replication", TEST_INITIAL_DFS_REPLICATION);
    doReturn(c).when(mockFs).getConf();
    jobPrototypeUnderTest.addReducePaths(targetNodePaths);

    jobPrototypeUnderTest.handleInitialReplication(testConfiguration);
    assertEquals(TEST_INITIAL_DFS_REPLICATION, testConfiguration.get("dfs.replication.initial"));
  }

  @Test
  public void initialReplicationFactorUsedWhenItCannotBeRetrievedFromFileSystem() throws IOException {
    testConfiguration = new Configuration();
    HashMultimap<Target, NodePath> targetNodePaths = HashMultimap.create();
    targetNodePaths.put(mockTarget, new NodePath());
    doReturn(mockPath).when(mockTarget).getPath();
    doThrow(new IOException()).when(mockPath).getFileSystem(any(Configuration.class));
    Configuration c = new Configuration();
    c.set("dfs.replication", TEST_INITIAL_DFS_REPLICATION);
    jobPrototypeUnderTest.addReducePaths(targetNodePaths);

    jobPrototypeUnderTest.handleInitialReplication(testConfiguration);
    assertEquals("3", testConfiguration.get("dfs.replication.initial"));  //default
  }
}
