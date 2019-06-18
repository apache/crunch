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
package org.apache.crunch.io;

import static org.hamcrest.CoreMatchers.is;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Assert;
import org.junit.Test;

public class FormatBundleTest {
  @Test
  public void testFileSystemConfs() throws Exception {
    Configuration fsConf = new Configuration(false);
    fsConf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///tmp/foo");
    fsConf.set("foo", "bar");
    fsConf.set("fs.fake.impl", "FakeFileSystem");
    fsConf.set("dfs.overridden", "fsValue");
    fsConf.set("dfs.extraOverridden", "fsExtra");
    fsConf.set(DFSConfigKeys.DFS_NAMESERVICES, "fs-cluster");

    FileSystem fs = FileSystem.newInstance(fsConf);

    FormatBundle<TextInputFormat> formatBundle = new FormatBundle<>(TextInputFormat.class);
    formatBundle.setFileSystem(fs);
    formatBundle.set("dfs.extraOverridden", "extraExtra");

    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, "pipeline-cluster");
    conf.set("dfs.overridden", "pipelineValue");
    formatBundle.configure(conf);

    // should be filtered by blacklist
    Assert.assertFalse(conf.get(FileSystem.FS_DEFAULT_NAME_KEY).equals("hdfs://my-hdfs"));

    // shouldn't be on whitelist
    Assert.assertFalse(conf.get("foo") != null);

    // should get through both blacklist and whitelist
    Assert.assertEquals("FakeFileSystem", conf.get("fs.fake.impl"));

    // should use value from fsConf
    Assert.assertEquals("fsValue", conf.get("dfs.overridden"));

    // should use value from 'extraConf'
    Assert.assertEquals("extraExtra", conf.get("dfs.extraOverridden"));

    // dfs.nameservices should be merged
    Assert.assertArrayEquals(new String [] {"pipeline-cluster", "fs-cluster"},
        conf.getStrings(DFSConfigKeys.DFS_NAMESERVICES));
  }
  @Test
  public void testRedactedFileSystemConfs() throws Exception {
    Configuration fsConf = new Configuration(false);
    fsConf.set("fs.s3a.access.key", "accessKey");
    fsConf.set("fs.s3a.secret.key", "secretKey");
    fsConf.set("fs.fake.impl", "FakeFileSystem");
    FileSystem fs = FileSystem.newInstance(fsConf);

    FormatBundle<TextInputFormat> formatBundle = new FormatBundle<>(TextInputFormat.class);
    formatBundle.setFileSystem(fs);

    Configuration conf = new Configuration();
    conf.set("mapreduce.job.redacted-properties", "fs.s3a.access.key,fs.s3a.secret.key");

    final FormatBundleTestAppender appender = new FormatBundleTestAppender();
    final Logger logger = Logger.getRootLogger();
    logger.addAppender(appender);
    try {
      Logger.getLogger(FormatBundleTest.class);
      formatBundle.configure(conf);
    } finally {
      logger.removeAppender(appender);
    }

    final List<LoggingEvent> log = appender.getLog();

    // redacted value: accesskey
    Assert.assertThat(log.get(0).getMessage().toString(),
        is("Applied fs.s3a.access.key=*********(redacted) from FS 'file:///'"));

    // fake non redacted value: fs.fake.impl
    Assert.assertThat(log.get(1).getMessage().toString(),
        is("Applied fs.fake.impl=FakeFileSystem from FS 'file:///'"));

    // redacted value: secretKey
    Assert.assertThat(log.get(2).getMessage().toString(),
        is("Applied fs.s3a.secret.key=*********(redacted) from FS 'file:///'"));
  }


  class FormatBundleTestAppender extends AppenderSkeleton {

    private final List<LoggingEvent> log = new ArrayList<>();

    @Override
    public boolean requiresLayout() {
      return false;
    }

    @Override
    protected void append(final LoggingEvent loggingEvent) {
      log.add(loggingEvent);
    }

    @Override
    public void close() {
    }

    public List<LoggingEvent> getLog() {
      return new ArrayList<>(log);
    }
  }

}