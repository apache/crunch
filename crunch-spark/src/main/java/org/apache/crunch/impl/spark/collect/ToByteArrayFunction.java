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
package org.apache.crunch.impl.spark.collect;

import org.apache.crunch.impl.spark.ByteArray;
import org.apache.crunch.impl.spark.IntByteArray;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class ToByteArrayFunction implements PairFunction<Tuple2<IntByteArray, List<byte[]>>, ByteArray, List<byte[]>> {
  @Override
  public Tuple2<ByteArray, List<byte[]>> call(Tuple2<IntByteArray, List<byte[]>> t) throws Exception {
    return new Tuple2<ByteArray, List<byte[]>>(t._1(), t._2());
  }
}
