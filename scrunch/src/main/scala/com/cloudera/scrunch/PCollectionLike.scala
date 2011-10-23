/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.scrunch

import com.cloudera.crunch.{DoFn, Emitter, FilterFn, MapFn}
import com.cloudera.crunch.{PCollection => JCollection, PTable => JTable, Pair => JPair, Target}
import com.cloudera.crunch.`type`.{PType, PTableType, PTypeFamily}
import com.cloudera.scrunch.Conversions._

trait PCollectionLike[S, +FullType, +NativeType <: JCollection[S]] {
  val native: NativeType
  
  def wrap(newNative: AnyRef): FullType
  
  def union(others: PCollectionLike[S, _, _]*) = {
    // TODO: fix this
    wrap(native.union(others.map(_.native.asInstanceOf[NativeType]): _*))
  }
  
  def ++(other: PCollectionLike[S, _, _]) = union(other)
  
  def write(target: Target): FullType = wrap(native.write(target))
  
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
  
  def getTypeFamily() = native.getTypeFamily()
}
