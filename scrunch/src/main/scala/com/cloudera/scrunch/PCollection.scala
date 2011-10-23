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
import com.cloudera.crunch.lib.Aggregate
import com.cloudera.crunch.`type`.{PType, PTableType, PTypeFamily}
import Conversions._

class PCollection[S](val native: JCollection[S]) extends PCollectionLike[S, PCollection[S], JCollection[S]] {

  def filter(f: S => Boolean): PCollection[S] = {
    parallelDo(new DSFilterFn[S](f), native.getPType())
  }

  def map[T, To](f: S => T)(implicit pt: PTypeH[T], b: CanParallelTransform[T, To]): To = {
    b(this, new DSMapFn[S, T](f), pt.getPType(getTypeFamily()))
  }

  def flatMap[T, To](f: S => Traversable[T])
      (implicit pt: PTypeH[T], b: CanParallelTransform[T, To]): To = {
    b(this, new DSDoFn[S, T](f), pt.getPType(getTypeFamily()))
  }

  def groupBy[K: PTypeH](f: S => K): PGroupedTable[K, S] = {
    val ptype = getTypeFamily().tableOf(implicitly[PTypeH[K]].getPType(getTypeFamily()), native.getPType())
    parallelDo(new DSMapKeyFn[S, K](f), ptype).groupByKey()
  }

  def wrap(newNative: AnyRef) = new PCollection[S](newNative.asInstanceOf[JCollection[S]])
  
  def count = new PTable[S, Long](Aggregate.count(native).asInstanceOf[JTable[S, Long]])
  
  def materialize() = new ConversionIterable[S](native.materialize())
}

trait SDoFn[S, T] extends DoFn[S, T] with Function1[S, Traversable[T]] {
  override def process(input: S, emitter: Emitter[T]): Unit = {
    for (v <- apply(c2s(input).asInstanceOf[S])) {
      emitter.emit(s2c(v).asInstanceOf[T])
    }
  }
}

trait SFilterFn[T] extends FilterFn[T] with Function1[T, Boolean] {
  override def accept(input: T): Boolean = {
    apply(c2s(input).asInstanceOf[T]);
  }
}

trait SMapFn[S, T] extends MapFn[S, T] with Function1[S, T] {
  override def map(input: S): T = {
    s2c(apply(c2s(input).asInstanceOf[S])).asInstanceOf[T]
  }
}

trait SMapKeyFn[S, K] extends MapFn[S, JPair[K, S]] with Function1[S, K] {
  override def map(input: S): JPair[K, S] = {
    val sc = c2s(input).asInstanceOf[S]
    JPair.of(s2c(apply(sc)).asInstanceOf[K], input)
  }
}

class DSDoFn[S, T](fn: S => Traversable[T]) extends SDoFn[S, T] {
  ClosureCleaner.clean(fn)
  override def apply(x: S) = fn(x)
}

class DSFilterFn[S](fn: S => Boolean) extends SFilterFn[S] {
  ClosureCleaner.clean(fn)
  override def apply(x: S) = fn(x)
}

class DSMapFn[S, T](fn: S => T) extends SMapFn[S, T] {
  ClosureCleaner.clean(fn)
  override def apply(x: S) = fn(x)
}

class DSMapKeyFn[S, K](fn: S => K) extends SMapKeyFn[S, K] {
  ClosureCleaner.clean(fn)
  override def apply(x: S) = fn(x)
}
