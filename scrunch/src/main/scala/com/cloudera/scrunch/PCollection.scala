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
import com.cloudera.crunch.{PCollection => JCollection, PTable => JTable, Pair => CPair, Target}
import com.cloudera.crunch.lib.Aggregate
import com.cloudera.scrunch.Conversions._
import scala.collection.JavaConversions

class PCollection[S](val native: JCollection[S]) extends PCollectionLike[S, PCollection[S], JCollection[S]] {
  import PCollection._

  def filter(f: S => Boolean): PCollection[S] = {
    parallelDo(filterFn[S](f), native.getPType())
  }

  def map[T, To](f: S => T)(implicit pt: PTypeH[T], b: CanParallelTransform[T, To]): To = {
    b(this, mapFn(f), pt.get(getTypeFamily()))
  }

  def flatMap[T, To](f: S => Traversable[T])
      (implicit pt: PTypeH[T], b: CanParallelTransform[T, To]): To = {
    b(this, flatMapFn(f), pt.get(getTypeFamily()))
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
  
  def materialize() = JavaConversions.iterableAsScalaIterable[S](native.materialize)

  def wrap(newNative: AnyRef) = new PCollection[S](newNative.asInstanceOf[JCollection[S]])
  
  def count() = {
    val count = new PTable[S, java.lang.Long](Aggregate.count(native))
    count.mapValues(_.longValue()) 
  }

  def max() = wrap(Aggregate.max(native))
}

trait SDoFn[S, T] extends DoFn[S, T] with Function1[S, Traversable[T]] {
  override def process(input: S, emitter: Emitter[T]) {
    for (v <- apply(input)) {
      emitter.emit(v)
    }
  }
}

trait SFilterFn[T] extends FilterFn[T] with Function1[T, Boolean] {
  override def accept(input: T) = apply(input)
}

trait SMapFn[S, T] extends MapFn[S, T] with Function1[S, T] {
  override def map(input: S) = apply(input)
}

trait SMapKeyFn[S, K] extends MapFn[S, CPair[K, S]] with Function1[S, K] {
  override def map(input: S): CPair[K, S] = {
    CPair.of(apply(input), input)
  }
}

object PCollection {
  def filterFn[S](fn: S => Boolean) = {
    new SFilterFn[S] { def apply(x: S) = fn(x) }
  }

  def mapKeyFn[S, K](fn: S => K) = {
    new SMapKeyFn[S, K] { def apply(x: S) = fn(x) }
  }

  def mapFn[S, T](fn: S => T) = {
    new SMapFn[S, T] { def apply(s: S) = fn(s) }
  }

  def flatMapFn[S, T](fn: S => Traversable[T]) = {
    new SDoFn[S, T] { def apply(s: S) = fn(s) }
  }
}
