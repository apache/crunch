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
package org.apache.crunch.lambda;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * Context object for implementing distributed operations in terms of Lambda expressions.
 * @param <S> Input type of LDoFn
 * @param <T> Output type of LDoFn
 */
public interface LDoFnContext<S, T> {
    /** Get the input element */
    S element();

    /** Emit t to the output */
    void emit(T t);

    /** Get the underlying {@link TaskInputOutputContext} (for special cases) */
    TaskInputOutputContext getContext();

    /** Get the current Hadoop {@link Configuration} */
    Configuration getConfiguration();

    /** Increment a counter by 1 */
    void increment(String groupName, String counterName);

    /** Increment a counter by value */
    void increment(String groupName, String counterName, long value);

    /** Increment a counter by 1 */
    void increment(Enum<?> counterName);

    /** Increment a counter by value */
    void increment(Enum<?> counterName, long value);
}
