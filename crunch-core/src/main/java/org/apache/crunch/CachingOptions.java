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
 * Options for controlling how a {@code PCollection<T>} is cached for subsequent processing. Different pipeline
 * execution frameworks may use some or all of these options when deciding how to cache a given {@code PCollection}
 * depending on the implementation details of the framework.
 */
public class CachingOptions {

  private final boolean useDisk;
  private final boolean useMemory;
  private final boolean deserialized;
  private final int replicas;

  private CachingOptions(
      boolean useDisk,
      boolean useMemory,
      boolean deserialized,
      int replicas) {
    this.useDisk = useDisk;
    this.useMemory = useMemory;
    this.deserialized = deserialized;
    this.replicas = replicas;
  }

  /**
   * Whether the framework may cache data on disk.
   */
  public boolean useDisk() {
    return useDisk;
  }

  /**
   * Whether the framework may cache data in memory without writing it to disk.
   */
  public boolean useMemory() {
    return useMemory;
  }

  /**
   * Whether the data should remain deserialized in the cache, which trades off CPU processing time
   * for additional storage overhead.
   */
  public boolean deserialized() {
    return deserialized;
  }

  /**
   * Returns the number of replicas of the data that should be maintained in the cache.
   */
  public int replicas() {
    return replicas;
  }

  /**
   * Creates a new {@link Builder} instance to use for specifying the caching options for a particular
   * {@code PCollection<T>}.
   * @return
   */
  public static Builder builder() {
    return new CachingOptions.Builder();
  }

  /**
   * An instance of {@code CachingOptions} with the default caching settings.
   */
  public static final CachingOptions DEFAULT = CachingOptions.builder().build();

  /**
   * A Builder class to use for setting the {@code CachingOptions} for a {@link PCollection}. The default
   * settings are to keep a single replica of the data deserialized in memory, without writing to disk
   * unless it is required due to resource limitations.
   */
  public static class Builder {
    private boolean useDisk = false;
    private boolean useMemory = true;
    private boolean deserialized = true;
    private int replicas = 1;

    public Builder() {}

    public Builder useMemory(boolean useMemory) {
      this.useMemory = useMemory;
      return this;
    }

    public Builder useDisk(boolean useDisk) {
      this.useDisk = useDisk;
      return this;
    }

    public Builder deserialized(boolean deserialized) {
      this.deserialized = deserialized;
      return this;
    }

    public Builder replicas(int replicas) {
      Preconditions.checkArgument(replicas > 0);
      this.replicas = replicas;
      return this;
    }

    public CachingOptions build() {
      return new CachingOptions(useDisk, useMemory, deserialized, replicas);
    }
  }
}
