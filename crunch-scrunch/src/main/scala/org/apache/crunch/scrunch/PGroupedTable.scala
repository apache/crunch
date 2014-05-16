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
import scala.collection.Iterable
import scala.collection.JavaConversions._
import org.apache.hadoop.mapreduce.TaskInputOutputContext

class PGroupedTable[K, V](val native: JGroupedTable[K, V])
    extends PCollectionLike[CPair[K, JIterable[V]], PGroupedTable[K, V], JGroupedTable[K, V]] {
  import PGroupedTable._

  type FunctionType[T] = (K, Iterable[V]) => T
  type CtxtFunctionType[T] = (K, Iterable[V], TIOC) => T

  protected def wrapFlatMapFn[T](fmt: (K, Iterable[V]) => TraversableOnce[T]) = flatMapFn(fmt)
  protected def wrapMapFn[T](fmt: (K, Iterable[V]) => T) = mapFn(fmt)
  protected def wrapFilterFn(fmt: (K, Iterable[V]) => Boolean) = filterFn(fmt)
  protected def wrapFlatMapWithCtxtFn[T](fmt: (K, Iterable[V], TIOC) => TraversableOnce[T]) = {
    flatMapWithCtxtFn(fmt)
  }
  protected def wrapMapWithCtxtFn[T](fmt: (K, Iterable[V], TIOC) => T) = mapWithCtxtFn(fmt)
  protected def wrapFilterWithCtxtFn(fmt: (K, Iterable[V], TIOC) => Boolean) = filterWithCtxtFn(fmt)
  protected def wrapPairFlatMapFn[S, T](fmt: (K, Iterable[V]) => TraversableOnce[(S, T)]) = pairFlatMapFn(fmt)
  protected def wrapPairMapFn[S, T](fmt: (K, Iterable[V]) => (S, T)) = pairMapFn(fmt)

  def combine(f: Iterable[V] => V) = combineValues(new IterableCombineFn[K, V](f))

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

class IterableCombineFn[K, V](f: Iterable[V] => V) extends CombineFn[K, V] {
  override def process(input: CPair[K, JIterable[V]], emitfn: Emitter[CPair[K, V]]) = {
    emitfn.emit(CPair.of(input.first(), f(iterableAsScalaIterable[V](input.second()))))
  }
}

trait SDoGroupedFn[K, V, T] extends DoFn[CPair[K, JIterable[V]], T] with Function2[K, Iterable[V], TraversableOnce[T]] {
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

trait SFilterGroupedFn[K, V] extends FilterFn[CPair[K, JIterable[V]]] with Function2[K, Iterable[V], Boolean] {
  override def accept(input: CPair[K, JIterable[V]]) = {
    apply(input.first(), iterableAsScalaIterable[V](input.second()))
  }
}

class SDoGroupedWithCtxtFn[K, V, T](val f: (K, Iterable[V], TaskInputOutputContext[_, _, _, _]) => TraversableOnce[T])
  extends DoFn[CPair[K, JIterable[V]], T] {
  override def process(input: CPair[K, JIterable[V]], emitter: Emitter[T]) {
    for (v <- f(input.first(), iterableAsScalaIterable[V](input.second()), getContext)) {
      emitter.emit(v)
    }
  }
}

class SMapGroupedWithCtxtFn[K, V, T](val f: (K, Iterable[V], TaskInputOutputContext[_, _, _, _]) => T)
  extends MapFn[CPair[K, JIterable[V]], T] {
  override def map(input: CPair[K, JIterable[V]]) = {
    f(input.first(), iterableAsScalaIterable[V](input.second()), getContext)
  }
}

class SFilterGroupedWithCtxtFn[K, V](val f: (K, Iterable[V], TaskInputOutputContext[_, _, _, _]) => Boolean)
  extends FilterFn[CPair[K, JIterable[V]]] {
  override def accept(input: CPair[K, JIterable[V]]) = {
    f.apply(input.first(), iterableAsScalaIterable[V](input.second()), getContext)
  }
}

trait SDoPairGroupedFn[K, V, S, T] extends DoFn[CPair[K, JIterable[V]], CPair[S, T]]
    with Function2[K, Iterable[V], TraversableOnce[(S, T)]] {
  override def process(input: CPair[K, JIterable[V]], emitter: Emitter[CPair[S, T]]) {
    for (v <- apply(input.first(), iterableAsScalaIterable[V](input.second()))) {
      emitter.emit(CPair.of(v._1, v._2))
    }
  }
}

trait SMapPairGroupedFn[K, V, S, T] extends MapFn[CPair[K, JIterable[V]], CPair[S, T]]
    with Function2[K, Iterable[V], (S, T)] {
  override def map(input: CPair[K, JIterable[V]]) = {
    val t = apply(input.first(), iterableAsScalaIterable[V](input.second()))
    CPair.of(t._1, t._2)
  }
}

object PGroupedTable {
  type TIOC = TaskInputOutputContext[_, _, _, _]

  def flatMapFn[K, V, T](fn: (K, Iterable[V]) => TraversableOnce[T]) = {
    new SDoGroupedFn[K, V, T] { def apply(k: K, v: Iterable[V]) = fn(k, v) }
  }

  def mapFn[K, V, T](fn: (K, Iterable[V]) => T) = {
    new SMapGroupedFn[K, V, T] { def apply(k: K, v: Iterable[V]) = fn(k, v) }
  }

  def filterFn[K, V](fn: (K, Iterable[V]) => Boolean) = {
    new SFilterGroupedFn[K, V] { def apply(k: K, v: Iterable[V]) = fn(k, v) }
  }

  def flatMapWithCtxtFn[K, V, T](fn: (K, Iterable[V], TIOC) => TraversableOnce[T]) = {
    new SDoGroupedWithCtxtFn[K, V, T](fn)
  }

  def mapWithCtxtFn[K, V, T](fn: (K, Iterable[V], TIOC) => T) = {
    new SMapGroupedWithCtxtFn[K, V, T](fn)
  }

  def filterWithCtxtFn[K, V](fn: (K, Iterable[V], TIOC) => Boolean) = {
    new SFilterGroupedWithCtxtFn[K, V](fn)
  }

  def pairMapFn[K, V, S, T](fn: (K, Iterable[V]) => (S, T)) = {
    new SMapPairGroupedFn[K, V, S, T] { def apply(k: K, v: Iterable[V]) = fn(k, v) }
  }

  def pairFlatMapFn[K, V, S, T](fn: (K, Iterable[V]) => TraversableOnce[(S, T)]) = {
    new SDoPairGroupedFn[K, V, S, T] { def apply(k: K, v: Iterable[V]) = fn(k, v) }
  }
}
