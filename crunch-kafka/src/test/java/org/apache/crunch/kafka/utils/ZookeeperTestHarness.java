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
 */

package org.apache.crunch.kafka.utils;

import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import java.io.IOException;

/**
 * A test harness that brings up an embedded Zookeeper instance.
 * <p>
 * Adapted from the {@code kafka.zk.ZooKeeperTestHarness} class.
 * </p>
 */
public class ZookeeperTestHarness {

  /**
   * Zookeeper connection info.
   */
  protected final String zookeeperConnect;

  private EmbeddedZookeeper zookeeper;
  private final int zkConnectionTimeout;
  private final int zkSessionTimeout;

  /**
   * Zookeeper client connection.
   */
  protected ZkUtils zkUtils;

  /**
   * Creates a new Zookeeper broker test harness.
   */
  public ZookeeperTestHarness() {
    this(KafkaTestUtils.getPorts(1)[0]);
  }

  /**
   * Creates a new Zookeeper service test harness using the given port.
   *
   * @param zookeeperPort The port number to use for Zookeeper client connections.
   */
  public ZookeeperTestHarness(int zookeeperPort) {
    this.zookeeper = null;
    this.zkUtils = null;
    this.zkConnectionTimeout = 6000;
    this.zkSessionTimeout = 6000;
    this.zookeeperConnect = "localhost:" + zookeeperPort;
  }

  /**
   * Returns a client for communicating with the Zookeeper service.
   *
   * @return A Zookeeper client.
   *
   * @throws IllegalStateException
   *             if Zookeeper has not yet been {@link #setUp()}, or has already been {@link #tearDown() torn down}.
   */
  public ZkClient getZkClient() {
    if (zkUtils == null) {
      throw new IllegalStateException("Zookeeper service is not active");
    }
    return zkUtils.zkClient();
  }

  public ZkUtils getZkUtils() {
    return zkUtils;
  }

  /**
   * Startup Zookeeper.
   *
   * @throws IOException if an error occurs during Zookeeper initialization.
   */
  public void setUp() throws IOException {
    zookeeper = new EmbeddedZookeeper(zookeeperConnect);
    ZkClient zkClient = new ZkClient(zookeeperConnect, zkSessionTimeout, zkConnectionTimeout, new ZkStringSerializer());
    ZkConnection connection = new ZkConnection(zookeeperConnect, zkSessionTimeout);
    zkUtils = new ZkUtils(zkClient, connection, false);
  }

  /**
   * Shutdown Zookeeper.
   */
  public void tearDown() throws IOException {
    if (zkUtils != null) {
      zkUtils.close();
      zkUtils = null;
    }
    if (zookeeper != null) {
      zookeeper.shutdown();
      zookeeper = null;
    }
  }
}
