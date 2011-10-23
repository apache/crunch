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

import com.cloudera.crunch.{DoFn, Emitter, FilterFn, MapFn, Target}
import com.cloudera.crunch.{GroupingOptions, PTable => JTable, Pair => JPair}
import com.cloudera.crunch.lib.Cogroup
import com.cloudera.scrunch.Conversions._
import scala.collection.JavaConversions._

class PTable[K, V](val native: JTable[K, V]) extends PCollectionLike[JPair[K, V], PTable[K, V], JTable[K, V]] {

  def filter(f: (K, V) => Boolean): PTable[K, V] = {
    parallelDo(new DSFilterTableFn[K, V](f), native.getPTableType())
  }

  def map[T, To](f: (K, V) => T)
      (implicit pt: PTypeH[T], b: CanParallelTransform[T, To]): To = {
    b(this, new DSMapTableFn[K, V, T](f), pt.getPType(getTypeFamily()))
  }

  def flatMap[T, To](f: (K, V) => Traversable[T])
      (implicit pt: PTypeH[T], b: CanParallelTransform[T, To]): To = {
    b(this, new DSDoTableFn[K, V, T](f), pt.getPType(getTypeFamily()))
  }

  def cogroup[V2](other: PTable[K, V2]) = {
    val jres = Cogroup.cogroup[K, V, V2](this.native, other.native)
    new PTable[K, (Iterable[V], Iterable[V2])](jres.asInstanceOf[JTable[K, (Iterable[V], Iterable[V2])]])
  }

  def groupByKey() = new PGroupedTable(native.groupByKey())

  def groupByKey(partitions: Int) = new PGroupedTable(native.groupByKey(partitions))

  def groupByKey(options: GroupingOptions) = new PGroupedTable(native.groupByKey(options))

  def wrap(newNative: AnyRef) = {
    new PTable[K, V](newNative.asInstanceOf[JTable[K, V]])
  }
}

trait SFilterTableFn[K, V] extends FilterFn[JPair[K, V]] with Function2[K, V, Boolean] {
  override def accept(input: JPair[K, V]): Boolean = {
    apply(c2s(input.first()).asInstanceOf[K], c2s(input.second()).asInstanceOf[V]);
  }
}

trait SDoTableFn[K, V, T] extends DoFn[JPair[K, V], T] with Function2[K, V, Traversable[T]] {
  override def process(input: JPair[K, V], emitter: Emitter[T]): Unit = {
    val k = c2s(input.first()).asInstanceOf[K]
    val v = c2s(input.second()).asInstanceOf[V]
    for (v <- apply(k, v)) {
      emitter.emit(s2c(v).asInstanceOf[T])
    }
  }
}

trait SMapTableFn[K, V, T] extends MapFn[JPair[K, V], T] with Function2[K, V, T] {
  override def map(input: JPair[K, V]): T = {
    val v = apply(c2s(input.first()).asInstanceOf[K], c2s(input.second()).asInstanceOf[V])
    s2c(v).asInstanceOf[T]
  }
}

class DSFilterTableFn[K, V](fn: (K, V) => Boolean) extends SFilterTableFn[K, V] {
  ClosureCleaner.clean(fn)
  override def apply(k: K, v: V) = fn(k, v)  
}

class DSDoTableFn[K, V, T](fn: (K, V) => Traversable[T]) extends SDoTableFn[K, V, T] {
  ClosureCleaner.clean(fn)
  override def apply(k: K, v: V) = fn(k, v)  
}

class DSMapTableFn[K, V, T](fn: (K, V) => T) extends SMapTableFn[K, V, T] {
  ClosureCleaner.clean(fn)
  override def apply(k: K, v: V) = fn(k, v)  
}
