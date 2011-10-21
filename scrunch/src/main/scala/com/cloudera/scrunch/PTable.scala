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
import com.cloudera.scrunch.Conversions._

class PTable[K, V](jtable: JTable[K, V]) extends PCollection[JPair[K, V]](jtable) with JTable[K, V] {

  def filter(f: (K, V) => Boolean): PTable[K, V] = {
    parallelDo(new DSFilterTableFn[K, V](f), getPTableType())
  }

  def map[T, To](f: (K, V) => T)
      (implicit pt: PTypeH[T], b: CanParallelTransform[PCollection[JPair[K, V]], T, To]): To = {
    b(this, new DSMapTableFn[K, V, T](f), pt.getPType(getTypeFamily()))
  }

  def flatMap[T, To](f: (K, V) => Traversable[T])
      (implicit pt: PTypeH[T], b: CanParallelTransform[PCollection[JPair[K, V]], T, To]): To = {
    b(this, new DSDoTableFn[K, V, T](f), pt.getPType(getTypeFamily()))
  }

  override def union(tables: JTable[K, V]*) = {
    new PTable[K, V](jtable.union(tables.map(baseCheck): _*))
  }

  private def baseCheck(c: JTable[K, V]): JTable[K, V] = c match {
    case x: PTable[K, V] => x.base.asInstanceOf[PTable[K, V]]
    case _ => c
  }

  def ++ (other: JTable[K, V]) = union(other)

  override def groupByKey() = new PGroupedTable(jtable.groupByKey())

  override def groupByKey(partitions: Int) = new PGroupedTable(jtable.groupByKey(partitions))

  override def groupByKey(options: GroupingOptions) = new PGroupedTable(jtable.groupByKey(options))

  override def write(target: Target) = {
    jtable.write(target)
    this
  }
  
  override def getPTableType() = jtable.getPTableType()

  override def getKeyType() = jtable.getKeyType()

  override def getValueType() = jtable.getValueType()
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
