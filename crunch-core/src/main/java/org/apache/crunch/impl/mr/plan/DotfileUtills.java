/*
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

import org.apache.hadoop.conf.Configuration;

/**
 * Helper class that manages the dotfile generation lifecycle and configuring the dotfile debug context.
 *
 * @deprecated use {@link DotfileUtil} instead
 */
public class DotfileUtills {

  public static boolean isDebugDotfilesEnabled(Configuration conf) {
    return DotfileUtil.isDebugDotfilesEnabled(conf);
  }

  public static void enableDebugDotfiles(Configuration conf) {
    DotfileUtil.enableDebugDotfiles(conf);
  }

  public static void disableDebugDotfilesEnabled(Configuration conf) {
    DotfileUtil.disableDebugDotfiles(conf);
  }

  public static void setPipelineDotfileOutputDir(Configuration conf, String outputDir) {
    DotfileUtil.setPipelineDotfileOutputDir(conf, outputDir);
  }

  public static String getPipelineDotfileOutputDir(Configuration conf) {
    return DotfileUtil.getPipelineDotfileOutputDir(conf);
  }
}
