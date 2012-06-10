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
import com.cloudera.crunch.{GroupingOptions, PTable => JTable, Pair => CPair}
import com.cloudera.crunch.lib.{Join, Aggregate, Cogroup, PTables}
import java.util.{Collection => JCollect}
import scala.collection.JavaConversions._

class PTable[K, V](val native: JTable[K, V]) extends PCollectionLike[CPair[K, V], PTable[K, V], JTable[K, V]] {
  import PTable._

  def filter(f: (K, V) => Boolean): PTable[K, V] = {
    parallelDo(filterFn[K, V](f), native.getPTableType())
  }

  def map[T, To](f: (K, V) => T)
      (implicit pt: PTypeH[T], b: CanParallelTransform[T, To]): To = {
    b(this, mapFn(f), pt.get(getTypeFamily()))
  }

  def mapValues[T](f: V => T)(implicit pt: PTypeH[T]) = {
    val ptf = getTypeFamily()
    val ptype = ptf.tableOf(native.getKeyType(), pt.get(ptf))
    parallelDo(mapValuesFn[K, V, T](f), ptype)
  }

  def flatMap[T, To](f: (K, V) => Traversable[T])
      (implicit pt: PTypeH[T], b: CanParallelTransform[T, To]): To = {
    b(this, flatMapFn(f), pt.get(getTypeFamily()))
  }

  def union(others: PTable[K, V]*) = {
    new PTable[K, V](native.union(others.map(_.native) : _*))
  }

  def keys() = new PCollection[K](PTables.keys(native))

  def values() = new PCollection[V](PTables.values(native))

  def cogroup[V2](other: PTable[K, V2]) = {
    val jres = Cogroup.cogroup[K, V, V2](this.native, other.native)
    val ptf = getTypeFamily()
    val inter = new PTable[K, CPair[JCollect[V], JCollect[V2]]](jres)
    inter.parallelDo(new SMapTableValuesFn[K, CPair[JCollect[V], JCollect[V2]], (Iterable[V], Iterable[V2])] {
      def apply(x: CPair[JCollect[V], JCollect[V2]]) = {
        (collectionAsScalaIterable[V](x.first()), collectionAsScalaIterable[V2](x.second()))
      }
    }, ptf.tableOf(keyType, ptf.tuple2(ptf.collections(valueType), ptf.collections(other.valueType))))
  }

  type JoinFn[V2] = (JTable[K, V], JTable[K, V2]) => JTable[K, CPair[V, V2]]

  protected def join[V2](joinFn: JoinFn[V2], other: PTable[K, V2]): PTable[K, (V, V2)] = {
    val jres = joinFn(this.native, other.native)
    val ptf = getTypeFamily()
    val ptype = ptf.tableOf(keyType, ptf.tuple2(valueType, other.valueType))
    val inter = new PTable[K, CPair[V, V2]](jres)
    inter.parallelDo(new SMapTableValuesFn[K, CPair[V, V2], (V, V2)] {
      def apply(x: CPair[V, V2]) = (x.first(), x.second())
    }, ptype)
  }

  def join[V2](other: PTable[K, V2]): PTable[K, (V, V2)] = {
    innerJoin(other)
  }

  def innerJoin[V2](other: PTable[K, V2]): PTable[K, (V, V2)] = {
    join[V2](Join.innerJoin[K, V, V2](_, _), other)
  }

  def leftJoin[V2](other: PTable[K, V2]): PTable[K, (V, V2)] = {
    join[V2](Join.leftJoin[K, V, V2](_, _), other)
  }

  def rightJoin[V2](other: PTable[K, V2]): PTable[K, (V, V2)] = {
    join[V2](Join.rightJoin[K, V, V2](_, _), other)
  }

  def fullJoin[V2](other: PTable[K, V2]): PTable[K, (V, V2)] = {
    join[V2](Join.fullJoin[K, V, V2](_, _), other)
  }

  def top(limit: Int, maximize: Boolean) = {
    wrap(Aggregate.top(this.native, limit, maximize))
  }

  def groupByKey() = new PGroupedTable(native.groupByKey())

  def groupByKey(partitions: Int) = new PGroupedTable(native.groupByKey(partitions))

  def groupByKey(options: GroupingOptions) = new PGroupedTable(native.groupByKey(options))

  def wrap(newNative: AnyRef) = {
    new PTable[K, V](newNative.asInstanceOf[JTable[K, V]])
  }
 
  def unwrap(sc: PTable[K, V]): JTable[K, V] = sc.native
 
  def materialize(): Iterable[(K, V)] = {
    native.materialize.view.map(x => (x.first, x.second))
  }

  def keyType() = native.getPTableType().getKeyType()

  def valueType() = native.getPTableType().getValueType()
}

trait SFilterTableFn[K, V] extends FilterFn[CPair[K, V]] with Function2[K, V, Boolean] {
  override def accept(input: CPair[K, V]) = apply(input.first(), input.second())
}

trait SDoTableFn[K, V, T] extends DoFn[CPair[K, V], T] with Function2[K, V, Traversable[T]] {
  override def process(input: CPair[K, V], emitter: Emitter[T]) {
    for (v <- apply(input.first(), input.second())) {
      emitter.emit(v)
    }
  }
}

trait SMapTableFn[K, V, T] extends MapFn[CPair[K, V], T] with Function2[K, V, T] {
  override def map(input: CPair[K, V]) = apply(input.first(), input.second())
}

trait SMapTableValuesFn[K, V, T] extends MapFn[CPair[K, V], CPair[K, T]] with Function1[V, T] {
  override def map(input: CPair[K, V]) = CPair.of(input.first(), apply(input.second()))
}

object PTable {
  def filterFn[K, V](fn: (K, V) => Boolean) = {
    new SFilterTableFn[K, V] { def apply(k: K, v: V) = fn(k, v) }
  }

  def mapValuesFn[K, V, T](fn: V => T) = {
    new SMapTableValuesFn[K, V, T] { def apply(v: V) = fn(v) }
  }

  def mapFn[K, V, T](fn: (K, V) => T) = {
    new SMapTableFn[K, V, T] { def apply(k: K, v: V) = fn(k, v) }
  }

  def flatMapFn[K, V, T](fn: (K, V) => Traversable[T]) = {
    new SDoTableFn[K, V, T] { def apply(k: K, v: V) = fn(k, v) }
  }
}
