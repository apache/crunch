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

import scala.collection.JavaConversions

import org.apache.crunch.{PCollection => JCollection, Pair => CPair, _}
import org.apache.crunch.lib.{Aggregate, Cartesian, Sample}
import org.apache.crunch.scrunch.Conversions._
import org.apache.crunch.scrunch.interpreter.InterpreterRunner
import org.apache.hadoop.mapreduce.TaskInputOutputContext
import org.apache.crunch.types.PType
import org.apache.crunch.fn.IdentityFn

class PCollection[S](val native: JCollection[S]) extends PCollectionLike[S, PCollection[S], JCollection[S]]
  with Incrementable[PCollection[S]] {

  import PCollection._

  type FunctionType[T] = S => T
  type CtxtFunctionType[T] = (S, TIOC) => T

  protected def wrapFlatMapFn[T](fmt: S => TraversableOnce[T]) = flatMapFn(fmt)
  protected def wrapMapFn[T](fmt: S => T) = mapFn(fmt)
  protected def wrapFilterFn(fmt: S => Boolean) = filterFn(fmt)
  protected def wrapFlatMapWithCtxtFn[T](fmt: (S, TIOC) => TraversableOnce[T]) = flatMapWithCtxtFn(fmt)
  protected def wrapMapWithCtxtFn[T](fmt: (S, TIOC) => T) = mapWithCtxtFn(fmt)
  protected def wrapFilterWithCtxtFn(fmt: (S, TIOC) => Boolean) = filterWithCtxtFn(fmt)
  protected def wrapPairFlatMapFn[K, V](fmt: S => TraversableOnce[(K, V)]) = pairFlatMapFn(fmt)
  protected def wrapPairMapFn[K, V](fmt: S => (K, V)) = pairMapFn(fmt)

  def withPType(pt: PType[S]): PCollection[S] = {
    val ident: MapFn[S, S] = IdentityFn.getInstance()
    wrap(native.parallelDo("withPType", ident, pt))
  }

  def union(others: PCollection[S]*) = {
    new PCollection[S](native.union(others.map(_.native) : _*))
  }

  def by[K: PTypeH](f: S => K): PTable[K, S] = {
    val ptype = getTypeFamily().tableOf(implicitly[PTypeH[K]].get(getTypeFamily()), native.getPType())
    parallelDo(mapKeyFn[S, K](f), ptype)
  }

  def groupBy[K: PTypeH](f: S => K): PGroupedTable[K, S] = {
    by(f).groupByKey
  }

  def cross[S2](other: PCollection[S2]): PCollection[(S, S2)] = {
    val inter = Cartesian.cross(this.native, other.native)
    val f = (in: CPair[S, S2]) => (in.first(), in.second())
    inter.parallelDo(mapFn(f), getTypeFamily().tuple2(pType, other.pType))
  }

  def materialize() = {
    setupRun()
    JavaConversions.iterableAsScalaIterable[S](native.materialize)
  }

  protected def wrap(newNative: JCollection[_]) = new PCollection[S](newNative.asInstanceOf[JCollection[S]])

  def collect[T, To](pf: PartialFunction[S, T])(implicit pt: PTypeH[T], b: CanParallelTransform[T, To]) = {
    filter(pf.isDefinedAt(_)).map(pf)(pt, b)
  }

  def increment(groupName: String, counterName: String, count: Long) = {
    new IncrementPCollection[S](this).apply(groupName, counterName, count)
  }

  def incrementIf(f: S => Boolean) = new IncrementIfPCollection[S](this, f)

  def count() = {
    val count = new PTable[S, java.lang.Long](Aggregate.count(native))
    count.mapValues(_.longValue())
  }

  def max()(implicit converter: Converter[S, S]) = {
    setupRun()
    PObject(Aggregate.max(native))(converter)
  }

  def min()(implicit converter: Converter[S, S]) = {
    setupRun()
    PObject(Aggregate.min(native))(converter)
  }

  def sample(acceptanceProbability: Double) = {
    wrap(Sample.sample(native, acceptanceProbability))
  }

  def sample(acceptanceProbability: Double, seed: Long) = {
    wrap(Sample.sample(native, seed, acceptanceProbability))
  }

  def pType() = native.getPType()
}

trait SDoFn[S, T] extends DoFn[S, T] with Function1[S, TraversableOnce[T]] {
  override def process(input: S, emitter: Emitter[T]) {
    for (v <- apply(input)) {
      emitter.emit(v)
    }
  }
}

trait SMapFn[S, T] extends MapFn[S, T] with Function1[S, T] {
  override def map(input: S) = apply(input)
}

trait SFilterFn[T] extends FilterFn[T] with Function1[T, Boolean] {
  override def accept(input: T) = apply(input)
}

class SDoWithCtxtFn[S, T](val f: (S, TaskInputOutputContext[_, _, _, _]) => TraversableOnce[T]) extends DoFn[S, T] {
  override def process(input: S, emitter: Emitter[T]) {
    for (v <- f(input, getContext)) {
      emitter.emit(v)
    }
  }
}

class SMapWithCtxtFn[S, T](val f: (S, TaskInputOutputContext[_, _, _, _]) => T) extends MapFn[S, T] {
  override def map(input: S) = f(input, getContext)
}

class SFilterWithCtxtFn[T](val f: (T, TaskInputOutputContext[_, _, _, _]) => Boolean) extends FilterFn[T] {
  override def accept(input: T) = f.apply(input, getContext)
}

trait SDoPairFn[S, K, V] extends DoFn[S, CPair[K, V]] with Function1[S, TraversableOnce[(K, V)]] {
  override def process(input: S, emitter: Emitter[CPair[K, V]]) {
    for (v <- apply(input)) {
      emitter.emit(CPair.of(v._1, v._2))
    }
  }
}

trait SMapPairFn[S, K, V] extends MapFn[S, CPair[K, V]] with Function1[S, (K, V)] {
  override def map(input: S): CPair[K, V] = {
    val t = apply(input)
    CPair.of(t._1, t._2)
  }
}

trait SMapKeyFn[S, K] extends MapFn[S, CPair[K, S]] with Function1[S, K] {
  override def map(input: S): CPair[K, S] = {
    CPair.of(apply(input), input)
  }
}

object PCollection {
  type TIOC = TaskInputOutputContext[_, _, _, _]

  def flatMapFn[S, T](fn: S => TraversableOnce[T]) = {
    new SDoFn[S, T] { def apply(s: S) = fn(s) }
  }

  def mapFn[S, T](fn: S => T) = {
    new SMapFn[S, T] { def apply(s: S) = fn(s) }
  }

  def filterFn[S](fn: S => Boolean) = {
    new SFilterFn[S] { def apply(x: S) = fn(x) }
  }

  def flatMapWithCtxtFn[S, T](fn: (S, TIOC) => TraversableOnce[T]) = {
    new SDoWithCtxtFn[S, T](fn)
  }

  def mapWithCtxtFn[S, T](fn: (S, TIOC) => T) = {
    new SMapWithCtxtFn[S, T](fn)
  }

  def filterWithCtxtFn[S](fn: (S, TIOC) => Boolean) = {
    new SFilterWithCtxtFn[S](fn)
  }

  def mapKeyFn[S, K](fn: S => K) = {
    new SMapKeyFn[S, K] { def apply(x: S) = fn(x) }
  }

  def pairMapFn[S, K, V](fn: S => (K, V)) = {
    new SMapPairFn[S, K, V] { def apply(s: S) = fn(s) }
  }

  def pairFlatMapFn[S, K, V](fn: S => TraversableOnce[(K, V)]) = {
    new SDoPairFn[S, K, V] { def apply(s: S) = fn(s) }
  }
}
