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
package org.apache.crunch.test;

import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 * Allows direct manipulation of the Hadoop log4j settings to aid in
 * unit testing. Not recommended for production use.
 */
public final class DebugLogging {

  /**
   * Enables logging Hadoop output to the console using the pattern
   * '%-4r [%t] %-5p %c %x - %m%n' at the specified {@code Level}.
   * 
   * @param level The log4j level
   */
  public static void enable(Level level) {
    enable(level, new ConsoleAppender(new PatternLayout("%-4r [%t] %-5p %c %x - %m%n")));
  }
  
  /**
   * Enables logging to the given {@code Appender} at the specified {@code Level}.
   * 
   * @param level The log4j level
   * @param appender The log4j appender
   */
  public static void enable(Level level, Appender appender) {
    Logger hadoopLogger = LogManager.getLogger("org.apache.hadoop");
    hadoopLogger.setLevel(level);
    hadoopLogger.addAppender(appender);
  }
  
  private DebugLogging() { }
}
