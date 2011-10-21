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
import com.cloudera.crunch.fn.IdentityFn
import com.cloudera.crunch.`type`.{PType, PTableType, PTypeFamily}
import com.cloudera.scrunch.Conversions._

class PCollection[S](jcollect: JCollection[S]) extends JCollection[S] {

  def filter(f: S => Boolean): PCollection[S] = {
    parallelDo(new DSFilterFn[S](f), getPType())
  }

  def map[T, To](f: S => T)(implicit pt: PTypeH[T], b: CanParallelTransform[PCollection[S], T, To]): To = {
    b(this, new DSMapFn[S, T](f), pt.getPType(getTypeFamily()))
  }

  def flatMap[T, To](f: S => Traversable[T])
      (implicit pt: PTypeH[T], b: CanParallelTransform[PCollection[S], T, To]): To = {
    b(this, new DSDoFn[S, T](f), pt.getPType(getTypeFamily()))
  }

  def groupBy[K: PTypeH](f: S => K): PGroupedTable[K, S] = {
    val ptype = getTypeFamily().tableOf(implicitly[PTypeH[K]].getPType(getTypeFamily()), getPType())
    parallelDo(new DSMapKeyFn[S, K](f), ptype).groupByKey()
  }

  protected def createPTableType[K, V](k: ClassManifest[K], v: ClassManifest[V]) = {
    getTypeFamily().tableOf(createPType(k), createPType(v))
  }

  protected def createPType[T](m: ClassManifest[T]): PType[T] = {
    manifest2PType(m, getTypeFamily()).asInstanceOf[PType[T]]
  }

  override def getPipeline() = jcollect.getPipeline()

  override def union(others: JCollection[S]*) = {
    new PCollection[S](jcollect.union(others.map(baseCheck): _*))
  }

  def base: JCollection[S] = jcollect match {
    case x: PCollection[S] => x.base
    case _ => jcollect
  }

  protected def baseCheck(collect: JCollection[S]) = collect match {
    case x: PCollection[S] => x.base
    case _ => collect
  }

  def ++ (other: JCollection[S]) = union(other)

  override def parallelDo[T](fn: DoFn[S, T], ptype: PType[T]) = {
    new PCollection[T](jcollect.parallelDo(fn, ptype))
  }

  override def parallelDo[T](name: String, fn: DoFn[S,T], ptype: PType[T]) = {
    new PCollection[T](jcollect.parallelDo(name, fn, ptype))
  }

  override def parallelDo[K, V](fn: DoFn[S, JPair[K, V]],
      ptype: PTableType[K, V]) = {
    new PTable[K, V](jcollect.parallelDo(fn, ptype))
  }

  override def parallelDo[K, V](name: String,
      fn: DoFn[S, JPair[K, V]], ptype: PTableType[K, V]) = {
    new PTable[K, V](jcollect.parallelDo(name, fn, ptype))
  }

  override def materialize(): java.lang.Iterable[S] = jcollect.materialize()

  override def write(target: Target) = {
    jcollect.write(target)
    this
  }

  override def getPType() = jcollect.getPType()

  override def getTypeFamily() = jcollect.getTypeFamily()

  override def getSize() = jcollect.getSize()

  override def getName() = jcollect.getName()
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
