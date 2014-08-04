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

import org.apache.crunch.{PCollection => JCollection, PGroupedTable => JGroupedTable, Pair => CPair, _}
import java.lang.{Iterable => JIterable}
import scala.collection.JavaConversions._
import org.apache.hadoop.mapreduce.TaskInputOutputContext

class PGroupedTable[K, V](val native: JGroupedTable[K, V])
    extends PCollectionLike[CPair[K, JIterable[V]], PGroupedTable[K, V], JGroupedTable[K, V]] {
  import PGroupedTable._

  type FunctionType[T] = (K, TraversableOnce[V]) => T
  type CtxtFunctionType[T] = (K, TraversableOnce[V], TIOC) => T

  protected def wrapFlatMapFn[T](fmt: (K, TraversableOnce[V]) => TraversableOnce[T]) = flatMapFn(fmt)
  protected def wrapMapFn[T](fmt: (K, TraversableOnce[V]) => T) = mapFn(fmt)
  protected def wrapFilterFn(fmt: (K, TraversableOnce[V]) => Boolean) = filterFn(fmt)
  protected def wrapFlatMapWithCtxtFn[T](fmt: (K, TraversableOnce[V], TIOC) => TraversableOnce[T]) = {
    flatMapWithCtxtFn(fmt)
  }
  protected def wrapMapWithCtxtFn[T](fmt: (K, TraversableOnce[V], TIOC) => T) = mapWithCtxtFn(fmt)
  protected def wrapFilterWithCtxtFn(fmt: (K, TraversableOnce[V], TIOC) => Boolean) = filterWithCtxtFn(fmt)
  protected def wrapPairFlatMapFn[S, T](fmt: (K, TraversableOnce[V]) => TraversableOnce[(S, T)]) = pairFlatMapFn(fmt)
  protected def wrapPairMapFn[S, T](fmt: (K, TraversableOnce[V]) => (S, T)) = pairMapFn(fmt)

  def combine(f: TraversableOnce[V] => V) = combineValues(new TraversableOnceCombineFn[K, V](f))

  def combineValues(agg: Aggregator[V]) = new PTable[K, V](native.combineValues(agg))

  def combineValues(combineAgg: Aggregator[V], reduceAgg: Aggregator[V]) = {
    new PTable[K, V](native.combineValues(combineAgg, reduceAgg))
  }

  def combineValues(fn: CombineFn[K, V]) = new PTable[K, V](native.combineValues(fn))

  def ungroup() = new PTable[K, V](native.ungroup())

  protected def wrap(newNative: JCollection[_]): PGroupedTable[K, V] = {
    new PGroupedTable[K, V](newNative.asInstanceOf[JGroupedTable[K, V]])
  }
}

class TraversableOnceCombineFn[K, V](f: TraversableOnce[V] => V) extends CombineFn[K, V] {
  override def process(input: CPair[K, JIterable[V]], emitfn: Emitter[CPair[K, V]]) = {
    emitfn.emit(CPair.of(input.first(), f(iterableAsScalaIterable[V](input.second()).iterator)))
  }
}

trait SDoGroupedFn[K, V, T] extends DoFn[CPair[K, JIterable[V]], T] with Function2[K, TraversableOnce[V], TraversableOnce[T]] {
  override def process(input: CPair[K, JIterable[V]], emitter: Emitter[T]) {
    for (v <- apply(input.first(), iterableAsScalaIterable[V](input.second()).iterator)) {
      emitter.emit(v)
    }
  }
}

trait SMapGroupedFn[K, V, T] extends MapFn[CPair[K, JIterable[V]], T] with Function2[K, TraversableOnce[V], T] {
  override def map(input: CPair[K, JIterable[V]]) = {
    apply(input.first(), iterableAsScalaIterable[V](input.second()).iterator)
  }
}

trait SFilterGroupedFn[K, V] extends FilterFn[CPair[K, JIterable[V]]] with Function2[K, TraversableOnce[V], Boolean] {
  override def accept(input: CPair[K, JIterable[V]]) = {
    apply(input.first(), iterableAsScalaIterable[V](input.second()).iterator)
  }
}

class SDoGroupedWithCtxtFn[K, V, T](val f: (K, TraversableOnce[V], TaskInputOutputContext[_, _, _, _]) => TraversableOnce[T])
  extends DoFn[CPair[K, JIterable[V]], T] {
  override def process(input: CPair[K, JIterable[V]], emitter: Emitter[T]) {
    for (v <- f(input.first(), iterableAsScalaIterable[V](input.second()).iterator, getContext)) {
      emitter.emit(v)
    }
  }
}

class SMapGroupedWithCtxtFn[K, V, T](val f: (K, TraversableOnce[V], TaskInputOutputContext[_, _, _, _]) => T)
  extends MapFn[CPair[K, JIterable[V]], T] {
  override def map(input: CPair[K, JIterable[V]]) = {
    f(input.first(), iterableAsScalaIterable[V](input.second()).iterator, getContext)
  }
}

class SFilterGroupedWithCtxtFn[K, V](val f: (K, TraversableOnce[V], TaskInputOutputContext[_, _, _, _]) => Boolean)
  extends FilterFn[CPair[K, JIterable[V]]] {
  override def accept(input: CPair[K, JIterable[V]]) = {
    f.apply(input.first(), iterableAsScalaIterable[V](input.second()).iterator, getContext)
  }
}

trait SDoPairGroupedFn[K, V, S, T] extends DoFn[CPair[K, JIterable[V]], CPair[S, T]]
    with Function2[K, TraversableOnce[V], TraversableOnce[(S, T)]] {
  override def process(input: CPair[K, JIterable[V]], emitter: Emitter[CPair[S, T]]) {
    for (v <- apply(input.first(), iterableAsScalaIterable[V](input.second()).iterator)) {
      emitter.emit(CPair.of(v._1, v._2))
    }
  }
}

trait SMapPairGroupedFn[K, V, S, T] extends MapFn[CPair[K, JIterable[V]], CPair[S, T]]
    with Function2[K, TraversableOnce[V], (S, T)] {
  override def map(input: CPair[K, JIterable[V]]) = {
    val t = apply(input.first(), iterableAsScalaIterable[V](input.second()).iterator)
    CPair.of(t._1, t._2)
  }
}

object PGroupedTable {
  type TIOC = TaskInputOutputContext[_, _, _, _]

  def flatMapFn[K, V, T](fn: (K, TraversableOnce[V]) => TraversableOnce[T]) = {
    new SDoGroupedFn[K, V, T] { def apply(k: K, v: TraversableOnce[V]) = fn(k, v) }
  }

  def mapFn[K, V, T](fn: (K, TraversableOnce[V]) => T) = {
    new SMapGroupedFn[K, V, T] { def apply(k: K, v: TraversableOnce[V]) = fn(k, v) }
  }

  def filterFn[K, V](fn: (K, TraversableOnce[V]) => Boolean) = {
    new SFilterGroupedFn[K, V] { def apply(k: K, v: TraversableOnce[V]) = fn(k, v) }
  }

  def flatMapWithCtxtFn[K, V, T](fn: (K, TraversableOnce[V], TIOC) => TraversableOnce[T]) = {
    new SDoGroupedWithCtxtFn[K, V, T](fn)
  }

  def mapWithCtxtFn[K, V, T](fn: (K, TraversableOnce[V], TIOC) => T) = {
    new SMapGroupedWithCtxtFn[K, V, T](fn)
  }

  def filterWithCtxtFn[K, V](fn: (K, TraversableOnce[V], TIOC) => Boolean) = {
    new SFilterGroupedWithCtxtFn[K, V](fn)
  }

  def pairMapFn[K, V, S, T](fn: (K, TraversableOnce[V]) => (S, T)) = {
    new SMapPairGroupedFn[K, V, S, T] { def apply(k: K, v: TraversableOnce[V]) = fn(k, v) }
  }

  def pairFlatMapFn[K, V, S, T](fn: (K, TraversableOnce[V]) => TraversableOnce[(S, T)]) = {
    new SDoPairGroupedFn[K, V, S, T] { def apply(k: K, v: TraversableOnce[V]) = fn(k, v) }
  }
}
