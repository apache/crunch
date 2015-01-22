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

import com.google.common.base.Preconditions;

/**
 * Additional options that can be specified when creating a new PCollection using {@link Pipeline#create}.
 */
public class CreateOptions {

  public static CreateOptions none() {
    return new CreateOptions("CREATED", 1);
  }

  public static CreateOptions parallelism(int parallelism) {
    return new CreateOptions("CREATED", parallelism);
  }

  public static CreateOptions name(String name) {
    return new CreateOptions(name, 1);
  }

  public static CreateOptions nameAndParallelism(String name, int parallelism) {
    return new CreateOptions(name, parallelism);
  }

  private final String name;
  private final int parallelism;

  private CreateOptions(String name, int parallelism) {
    this.name = Preconditions.checkNotNull(name);
    Preconditions.checkArgument(parallelism > 0, "Invalid parallelism value = %d", parallelism);
    this.parallelism = parallelism;
  }

  public String getName() {
    return name;
  }

  public int getParallelism() {
    return parallelism;
  }
}
