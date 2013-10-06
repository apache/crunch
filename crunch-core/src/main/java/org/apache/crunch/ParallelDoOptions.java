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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;

/**
 * Container class that includes optional information about a {@code parallelDo} operation
 * applied to a {@code PCollection}. Primarily used within the Crunch framework
 * itself for certain types of advanced processing operations, such as in-memory joins
 * that require reading a file from the filesystem into a {@code DoFn}.
 */
public class ParallelDoOptions {
  private final Set<SourceTarget<?>> sourceTargets;
  private final Map<String, String> extraConf;

  private ParallelDoOptions(Set<SourceTarget<?>> sourceTargets, Map<String, String> extraConf) {
    this.sourceTargets = sourceTargets;
    this.extraConf = extraConf;
  }
  
  public Set<SourceTarget<?>> getSourceTargets() {
    return sourceTargets;
  }

  /**
   * Applies the key-value pairs that were associated with this instance to the given {@code Configuration}
   * object. This is called just before the {@code configure} method on the {@code DoFn} corresponding to this
   * instance is called, so it is possible for the {@code DoFn} to see (and possibly override) these settings.
   */
  public void configure(Configuration conf) {
    for (Map.Entry<String, String> e : extraConf.entrySet()) {
      conf.set(e.getKey(), e.getValue());
    }
  }

  public static Builder builder() {
    return new Builder();
  }
  
  public static class Builder {
    private Set<SourceTarget<?>> sourceTargets;
    private Map<String, String> extraConf;

    public Builder() {
      this.sourceTargets = Sets.newHashSet();
      this.extraConf = Maps.newHashMap();
    }
    
    public Builder sourceTargets(SourceTarget<?>... sourceTargets) {
      Collections.addAll(this.sourceTargets, sourceTargets);
      return this;
    }

    public Builder sourceTargets(Collection<SourceTarget<?>> sourceTargets) {
      this.sourceTargets.addAll(sourceTargets);
      return this;
    }

    /**
     * Specifies key-value pairs that should be added to the {@code Configuration} object associated with the
     * {@code Job} that includes these options.
     * @param confKey The key
     * @param confValue The value
     * @return This {@code Builder} instance
     */
    public Builder conf(String confKey, String confValue) {
      this.extraConf.put(confKey, confValue);
      return this;
    }

    public ParallelDoOptions build() {
      return new ParallelDoOptions(sourceTargets, extraConf);
    }
  }
}
