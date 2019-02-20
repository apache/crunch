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
package org.apache.crunch;

import static org.apache.hadoop.hdfs.MiniDFSCluster.HDFS_MINIDFS_BASEDIR;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.commons.io.IOUtils;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.test.TemporaryPaths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.Builder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Tests reading and writing from a FileSystem for which the Configuration is separate from the Pipeline's
 * Configuration.
 */
public class ExternalFilesystemIT {

    @ClassRule
    public static TemporaryPath tmpDir1 = TemporaryPaths.create();

    @ClassRule
    public static TemporaryPath tmpDir2 = TemporaryPaths.create();

    @ClassRule
    public static TemporaryPath tmpDir3 = TemporaryPaths.create();

    private static FileSystem dfsCluster1;
    private static FileSystem dfsCluster2;
    private static FileSystem defaultFs;

    private static Collection<MiniDFSCluster> miniDFSClusters = new ArrayList<>();

    @BeforeClass
    public static void setup() throws Exception {
        dfsCluster1 = createHaMiniClusterFs("cluster1", tmpDir1);
        dfsCluster2 = createHaMiniClusterFs("cluster2", tmpDir2);
        defaultFs = createHaMiniClusterFs("default", tmpDir3);
    }

    @AfterClass
    public static void teardown() throws IOException {
        dfsCluster1.close();
        dfsCluster2.close();
        defaultFs.close();
        for (MiniDFSCluster miniDFSCluster : miniDFSClusters) {
            miniDFSCluster.shutdown();
        }
    }

    @Test
    public void testReadWrite() throws Exception {
        // write a test file outside crunch
        Path path = new Path("hdfs://cluster1/input.txt");
        String testString = "Hello world!";
        try (PrintWriter printWriter = new PrintWriter(dfsCluster1.create(path, true))) {
            printWriter.println(testString);
        }

        // assert it can be read back using a Pipeline with config that doesn't know the FileSystem
        Iterable<String> strings = new MRPipeline(getClass(), minimalConfiguration())
            .read(From.textFile(path).fileSystem(dfsCluster1)).materialize();
        Assert.assertEquals(testString, concatStrings(strings));

        // write output with crunch using a Pipeline with config that doesn't know the FileSystem
        MRPipeline pipeline = new MRPipeline(getClass(), minimalConfiguration());
        PCollection<String> input = pipeline.read(From.textFile("hdfs://cluster1/input.txt").fileSystem(dfsCluster1));
        pipeline.write(input, To.textFile("hdfs://cluster2/output").fileSystem(dfsCluster2));
        pipeline.run();

        // assert the output was written correctly
        try (FSDataInputStream inputStream = dfsCluster2.open(new Path("hdfs://cluster2/output/out0-m-00000"))) {
            String readValue = IOUtils.toString(inputStream).trim();
            Assert.assertEquals(testString, readValue);
        }

        // make sure the clusters aren't getting mixed up
        Assert.assertFalse(dfsCluster1.exists(new Path("/output")));
    }

    /**
     * Tests that multiple calls to fileSystem() on Source, Target, or SourceTarget results in
     * IllegalStateException
     */
    @Test
    public void testResetFileSystem() {
        Source<String> source = From.textFile("/data").fileSystem(defaultFs);
        try {
            source.fileSystem(dfsCluster1);
            Assert.fail("Expected an IllegalStateException");
        } catch (IllegalStateException e) { }

        Target target = To.textFile("/data").fileSystem(defaultFs);
        try {
            target.fileSystem(dfsCluster1);
            Assert.fail("Expected an IllegalStateException");
        } catch (IllegalStateException e) { }

        SourceTarget<String> sourceTarget = target.asSourceTarget(source.getType());
        try {
            sourceTarget.fileSystem(dfsCluster1);
            Assert.fail("Expected an IllegalStateException");
        } catch (IllegalStateException e) { }
    }

    /**
     * Tests when supplied Filesystem is not in agreement with Path.  For example, Path is "hdfs://cluster1/data"
     * but FileSystem is hdfs://cluster2.
     */
    @Test
    public void testWrongFs() {
        Source<String> source = From.textFile("hdfs://cluster1/data");
        try {
            source.fileSystem(dfsCluster2);
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) { }

        Target target = To.textFile("hdfs://cluster1/data");
        try {
            target.fileSystem(dfsCluster2);
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) { }

        SourceTarget<String> sourceTarget = target.asSourceTarget(source.getType());
        try {
            sourceTarget.fileSystem(dfsCluster2);
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) { }
    }

    private static String concatStrings(Iterable<String> strings) {
        StringBuilder builder = new StringBuilder();
        for (String string : strings) {
            builder.append(string);
        }
        return builder.toString();
    }

    /**
     * Creates a minimal configuration pointing to an HA HDFS default filesystem to ensure
     * that configuration for external filesystems used in Sources and Targets doesn't mess up
     * dfs.nameservices on the Pipeline, which could cause the default filesystem to become
     * unresolveable.
     *
     * @return a minimal configuration with an HDFS HA default fs
     */
    private static Configuration minimalConfiguration() {
        Configuration minimalConfiguration = new Configuration(false);
        minimalConfiguration.addResource(defaultFs.getConf());
        minimalConfiguration.set("fs.defaultFS", "hdfs://default");
        // exposes bugs hidden by filesystem cache
        minimalConfiguration.set("fs.hdfs.impl.disable.cache", "true");
        return minimalConfiguration;
    }

    private static Configuration getDfsConf(String nsName, MiniDFSCluster cluster) {
        Configuration conf = new Configuration();
        conf.set("dfs.nameservices", nsName);
        conf.set("dfs.client.failover.proxy.provider." + nsName,
            "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        conf.set("dfs.ha.namenodes." + nsName, "nn1");
        conf.set("dfs.namenode.rpc-address." + nsName + ".nn1", "localhost:" + cluster.getNameNodePort());
        return conf;
    }

    private static FileSystem createHaMiniClusterFs(String clusterName, TemporaryPath temporaryPath)
        throws IOException {
        Configuration conf = new Configuration();
        conf.set(HDFS_MINIDFS_BASEDIR, temporaryPath.getRootFileName());
        MiniDFSCluster cluster = new Builder(conf).build();
        miniDFSClusters.add(cluster);
        return FileSystem.get(URI.create("hdfs://" + clusterName), getDfsConf(clusterName, cluster));
    }
}
