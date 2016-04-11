/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Assorted Kafka testing utility methods.
 */
public class KafkaTestUtils {

  private static final Random RANDOM = new Random();
  private static final String TEMP_DIR_PREFIX = "kafka-";

  private static final Set<Integer> USED_PORTS = new HashSet<Integer>();

  /**
   * Creates and returns a new randomly named temporary directory. It will be deleted upon JVM exit.
   *
   * @return a new temporary directory.
   *
   * @throws RuntimeException if a new temporary directory could not be created.
   */
  public static File getTempDir() {
    File file = new File(System.getProperty("java.io.tmpdir"), TEMP_DIR_PREFIX + RANDOM.nextInt(10000000));
    if (!file.mkdirs()) {
      throw new RuntimeException("could not create temp directory: " + file.getAbsolutePath());
    }
    file.deleteOnExit();
    return file;
  }

  /**
   * Returns an array containing the specified number of available local ports.
   *
   * @param count Number of local ports to identify and return.
   *
   * @return an array of available local port numbers.
   *
   * @throws RuntimeException if an I/O error occurs opening or closing a socket.
   */
  public static int[] getPorts(int count) {
    int[] ports = new int[count];
    Set<ServerSocket> openSockets = new HashSet<ServerSocket>(count + USED_PORTS.size());

    for (int i = 0; i < count; ) {
      try {
        ServerSocket socket = new ServerSocket(0);
        int port = socket.getLocalPort();
        openSockets.add(socket);

        // Disallow port reuse.
        if (!USED_PORTS.contains(port)) {
          ports[i++] = port;
          USED_PORTS.add(port);
        }
      } catch (IOException e) {
        throw new RuntimeException("could not open socket", e);
      }
    }

    // Close the sockets so that their port numbers can be used by the caller.
    for (ServerSocket socket : openSockets) {
      try {
        socket.close();
      } catch (IOException e) {
        throw new RuntimeException("could not close socket", e);
      }
    }

    return ports;
  }
}

