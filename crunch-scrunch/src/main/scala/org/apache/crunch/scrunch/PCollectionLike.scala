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
package org.apache.crunch.scrunch

import org.apache.crunch.DoFn
import org.apache.crunch.{PCollection => JCollection, Pair => JPair, Target}
import org.apache.crunch.types.{PType, PTableType}

trait PCollectionLike[S, +FullType, +NativeType <: JCollection[S]] {
  val native: NativeType

  def wrap(newNative: AnyRef): FullType

  def write(target: Target): FullType = wrap(native.write(target))

  def write(target: Target, writeMode: Target.WriteMode): FullType = {
    wrap(native.write(target, writeMode))
  }

  def parallelDo[T](fn: DoFn[S, T], ptype: PType[T]) = {
    new PCollection[T](native.parallelDo(fn, ptype))
  }

  def parallelDo[T](name: String, fn: DoFn[S,T], ptype: PType[T]) = {
    new PCollection[T](native.parallelDo(name, fn, ptype))
  }

  def parallelDo[K, V](fn: DoFn[S, JPair[K, V]], ptype: PTableType[K, V]) = {
    new PTable[K, V](native.parallelDo(fn, ptype))
  }

  def parallelDo[K, V](name: String, fn: DoFn[S, JPair[K, V]], ptype: PTableType[K, V]) = {
    new PTable[K, V](native.parallelDo(name, fn, ptype))
  }

  /**
   * Gets the number of elements represented by this PCollection.
   *
   * @return The number of elements in this PCollection.
   */
  def length(): PObject[Long] = {
    PObject(native.length())
  }

  def asSeq(): PObject[Seq[S]] = {
    PObject(native.asCollection())
  }

  def getTypeFamily() = Avros
}
