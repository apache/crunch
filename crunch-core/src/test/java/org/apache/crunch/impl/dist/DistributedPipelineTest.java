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
package org.apache.crunch.impl.dist;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DistributedPipelineTest {
    private final String testTempDirPath1 = "/tmp/crunch-1345424622/p1";
    private final String testTempDirPath2 = "/tmp/crunch-1345424622/p2";

    @Mock private Job mockJob;
    @Mock private Path mockPath;
    private Configuration testConfiguration = new Configuration();

    @Before
    public void setUp() {
        when(mockJob.getConfiguration()).thenReturn(testConfiguration);
    }

    @Test
    public void isTempDirFalseWhenCrunchCreatesNoDirs() {
        boolean isTmp = DistributedPipeline.isTempDir(mockJob, testTempDirPath1);
        Assert.assertFalse(isTmp);
    }

    @Test
    public void isTempDirTrueWhenFileIsInTempDir() {
        testConfiguration.set("crunch.tmp.dirs", "/tmp/crunch-1345424622/p1");
        boolean isTmp = DistributedPipeline.isTempDir(mockJob, testTempDirPath1);
        Assert.assertTrue(isTmp);
    }

    @Test
    public void isTempDirFalseWhenFileIsNotInTempDir() {
        testConfiguration.set("crunch.tmp.dirs", testTempDirPath1.toString());
        boolean isTmp = DistributedPipeline.isTempDir(mockJob, "/user/crunch/iwTV2/");
        Assert.assertFalse(isTmp);
    }

    @Test
    public void tempDirsAreStoredInPipelineConf() {
        DistributedPipeline distributedPipeline = Mockito.mock(DistributedPipeline.class, Mockito.CALLS_REAL_METHODS);
        Configuration testConfiguration = new Configuration();
        distributedPipeline.setConfiguration(testConfiguration);

        // no temp directory is present at startup
        Assert.assertEquals(
                null,
                distributedPipeline.getConfiguration().get("crunch.tmp.dirs"));

        // store a temp directory
        distributedPipeline.storeTempDirLocation(new Path(testTempDirPath1));
        Assert.assertEquals(
                testTempDirPath1.toString(),
                distributedPipeline.getConfiguration().get("crunch.tmp.dirs"));

        // store one more temp directory
        distributedPipeline.storeTempDirLocation(new Path(testTempDirPath2));
        Assert.assertEquals(
                String.format("%s:%s", testTempDirPath1.toString(), testTempDirPath2.toString()),
                distributedPipeline.getConfiguration().get("crunch.tmp.dirs"));

        // try to store the first temp directory again, not added again
        distributedPipeline.storeTempDirLocation(new Path(testTempDirPath1));
        Assert.assertEquals(
                String.format("%s:%s", testTempDirPath1.toString(), testTempDirPath2.toString()),
                distributedPipeline.getConfiguration().get("crunch.tmp.dirs"));
    }
}
