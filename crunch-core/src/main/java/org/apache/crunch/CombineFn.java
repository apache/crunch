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

import java.io.Serializable;
import java.math.BigInteger;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;

import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.util.Tuples;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * A special {@link DoFn} implementation that converts an {@link Iterable} of
 * values into a single value. If a {@code CombineFn} instance is used on a
 * {@link PGroupedTable}, the function will be applied to the output of the map
 * stage before the data is passed to the reducer, which can improve the runtime
 * of certain classes of jobs.
 * <p>
 * Note that the incoming {@code Iterable} can only be used to create an 
 * {@code Iterator} once. Calling {@link Iterable#iterator()} method a second
 * time will throw an {@link IllegalStateException}.
 */
public abstract class CombineFn<S, T> extends DoFn<Pair<S, Iterable<T>>, Pair<S, T>> {
}
