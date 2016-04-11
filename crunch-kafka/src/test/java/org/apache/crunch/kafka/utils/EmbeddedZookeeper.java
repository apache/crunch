/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * <p>
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.crunch.kafka.utils;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Embedded Zookeeper instance for testing purposes.
 * <p>
 * Adapted from the {@code kafka.zk.EmbeddedZookeeper} class.
 * </p>
 */
class EmbeddedZookeeper {

  private final File snapshotDir;
  private final File logDir;
  private final NIOServerCnxnFactory factory;

  /**
   * Constructs an embedded Zookeeper instance.
   *
   * @param connectString Zookeeper connection string.
   *
   * @throws IOException if an error occurs during Zookeeper initialization.
   */
  public EmbeddedZookeeper(String connectString) throws IOException {
    this.snapshotDir = KafkaTestUtils.getTempDir();
    this.logDir = KafkaTestUtils.getTempDir();
    this.factory = new NIOServerCnxnFactory();
    String hostname = connectString.split(":")[0];
    int port = Integer.valueOf(connectString.split(":")[1]);
    int maxClientConnections = 1024;
    factory.configure(new InetSocketAddress(hostname, port), maxClientConnections);
    try {
      int tickTime = 500;
      factory.startup(new ZooKeeperServer(snapshotDir, logDir, tickTime));
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  /**
   * Shuts down the embedded Zookeeper instance.
   */
  public void shutdown() throws IOException {
    factory.shutdown();
    FileUtils.deleteDirectory(snapshotDir);
    FileUtils.deleteDirectory(logDir);
  }
}

