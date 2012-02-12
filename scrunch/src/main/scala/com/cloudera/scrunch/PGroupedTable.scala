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
import com.cloudera.crunch.{CombineFn, PGroupedTable => JGroupedTable, PTable => JTable, Pair => CPair}
import java.lang.{Iterable => JIterable}
import scala.collection.{Iterable, Iterator}
import scala.collection.JavaConversions._
import Conversions._

class PGroupedTable[K, V](val native: JGroupedTable[K, V])
    extends PCollectionLike[CPair[K, JIterable[V]], PGroupedTable[K, V], JGroupedTable[K, V]] {
  import PGroupedTable._

  def filter(f: (K, Iterable[V]) => Boolean) = {
    parallelDo(filterFn[K, V](f), native.getPType())
  }

  def map[T, To](f: (K, Iterable[V]) => T)
      (implicit pt: PTypeH[T], b: CanParallelTransform[T, To]): To = {
    b(this, mapFn(f), pt.get(getTypeFamily()))
  }

  def flatMap[T, To](f: (K, Iterable[V]) => Traversable[T])
      (implicit pt: PTypeH[T], b: CanParallelTransform[T, To]): To = {
    b(this, flatMapFn(f), pt.get(getTypeFamily()))
  }

  def combine(f: Iterable[V] => V) = combineValues(new IterableCombineFn[K, V](f))

  def combineValues(fn: CombineFn[K, V]) = new PTable[K, V](native.combineValues(fn))

  def ungroup() = new PTable[K, V](native.ungroup())
  
  def wrap(newNative: AnyRef): PGroupedTable[K, V] = {
    new PGroupedTable[K, V](newNative.asInstanceOf[JGroupedTable[K, V]])
  }
}

class IterableCombineFn[K, V](f: Iterable[V] => V) extends CombineFn[K, V] {
  override def process(input: CPair[K, JIterable[V]], emitfn: Emitter[CPair[K, V]]) = {
    emitfn.emit(CPair.of(input.first(), f(iterableAsScalaIterable[V](input.second()))))
  }
}

trait SFilterGroupedFn[K, V] extends FilterFn[CPair[K, JIterable[V]]] with Function2[K, Iterable[V], Boolean] {
  override def accept(input: CPair[K, JIterable[V]]) = apply(input.first(), iterableAsScalaIterable[V](input.second()))
}

trait SDoGroupedFn[K, V, T] extends DoFn[CPair[K, JIterable[V]], T] with Function2[K, Iterable[V], Traversable[T]] {
  override def process(input: CPair[K, JIterable[V]], emitter: Emitter[T]) {
    for (v <- apply(input.first(), iterableAsScalaIterable[V](input.second()))) {
      emitter.emit(v)
    }
  }
}

trait SMapGroupedFn[K, V, T] extends MapFn[CPair[K, JIterable[V]], T] with Function2[K, Iterable[V], T] {
  override def map(input: CPair[K, JIterable[V]]) = {
    apply(input.first(), iterableAsScalaIterable[V](input.second()))
  }
}

object PGroupedTable {
  def filterFn[K, V](fn: (K, Iterable[V]) => Boolean) = {
    new SFilterGroupedFn[K, V] { def apply(k: K, v: Iterable[V]) = fn(k, v) }
  }

  def mapFn[K, V, T](fn: (K, Iterable[V]) => T) = {
    new SMapGroupedFn[K, V, T] { def apply(k: K, v: Iterable[V]) = fn(k, v) }
  }

  def flatMapFn[K, V, T](fn: (K, Iterable[V]) => Traversable[T]) = {
    new SDoGroupedFn[K, V, T] { def apply(k: K, v: Iterable[V]) = fn(k, v) }
  }
}
