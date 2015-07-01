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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import java.io.Serializable;

/**
 * A Java lambdas friendly version of the {@link DoFn} class.
 */
public interface IDoFn<S, T> extends Serializable {

  void process(Context<S, T> context);

  public interface Context<S, T> {
    S element();

    void emit(T t);

    TaskInputOutputContext getContext();

    Configuration getConfiguration();

    void increment(String groupName, String counterName);

    void increment(String groupName, String counterName, long value);

    void increment(Enum<?> counterName);

    void increment(Enum<?> counterName, long value);
  }
}
