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
package org.apache.crunch.impl.spark.fn;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.apache.crunch.DoFn;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.spark.GuavaUtils;
import org.apache.crunch.impl.spark.SparkRuntimeContext;
import org.apache.crunch.util.DoFnIterator;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.util.Iterator;

public class FlatMapIndexFn<S, T> implements Function2<Integer, Iterator, Iterator<T>> {
  private final DoFn<S, T> fn;
  private final boolean convertInput;
  private final SparkRuntimeContext ctxt;

  public FlatMapIndexFn(DoFn<S, T> fn, boolean convertInput, SparkRuntimeContext ctxt) {
    this.fn = fn;
    this.convertInput = convertInput;
    this.ctxt = ctxt;
  }

  @Override
  public Iterator<T> call(Integer partitionId, Iterator input) throws Exception {
    ctxt.initialize(fn, partitionId);
    Iterator in = convertInput ? Iterators.transform(input, GuavaUtils.tuple2PairFunc()) : input;
    return new DoFnIterator<S, T>(in, fn);
  }
}
