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

import java.lang.{Iterable => JIterable}
import java.util.{Collection => JCollect}

import scala.collection.JavaConversions._

import org.apache.crunch.{PCollection => JCollection, PTable => JTable, Pair => CPair, _}
import org.apache.crunch.lib._
import org.apache.crunch.lib.join.{JoinUtils, DefaultJoinStrategy, JoinType}
import org.apache.crunch.types.{PTableType, PType}
import scala.collection.Iterable
import org.apache.hadoop.mapreduce.TaskInputOutputContext
import org.apache.crunch.fn.IdentityFn

class PTable[K, V](val native: JTable[K, V]) extends PCollectionLike[CPair[K, V], PTable[K, V], JTable[K, V]]
  with Incrementable[PTable[K, V]] {
  import PTable._

  type FunctionType[T] = (K, V) => T
  type CtxtFunctionType[T] = (K, V, TIOC) => T

  protected def wrapFlatMapFn[T](fmt: (K, V) => TraversableOnce[T]) = flatMapFn(fmt)
  protected def wrapMapFn[T](fmt: (K, V) => T) = mapFn(fmt)
  protected def wrapFilterFn(fmt: (K, V) => Boolean) = filterFn(fmt)
  protected def wrapFlatMapWithCtxtFn[T](fmt: (K, V, TIOC) => TraversableOnce[T]) = flatMapWithCtxtFn(fmt)
  protected def wrapMapWithCtxtFn[T](fmt: (K, V, TIOC) => T) = mapWithCtxtFn(fmt)
  protected def wrapFilterWithCtxtFn(fmt: (K, V, TIOC) => Boolean) = filterWithCtxtFn(fmt)
  protected def wrapPairFlatMapFn[S, T](fmt: (K, V) => TraversableOnce[(S, T)]) = pairFlatMapFn(fmt)
  protected def wrapPairMapFn[S, T](fmt: (K, V) => (S, T)) = pairMapFn(fmt)

  def withPType(pt: PTableType[K, V]): PTable[K, V] = {
    val ident: MapFn[CPair[K, V], CPair[K, V]]  = IdentityFn.getInstance()
    wrap(native.parallelDo("withPType", ident, pt))
  }

  def collect[T, To](pf: PartialFunction[(K, V), T])(implicit pt: PTypeH[T], b: CanParallelTransform[T, To]) = {
    filter((k, v) => pf.isDefinedAt((k, v))).map((k, v) => pf((k, v)))(pt, b)
  }

  def secondarySortAndMap[K2, VX, T, To](f: (K, Iterable[(K2, VX)]) => T, numReducers: Int = -1)
    (implicit ev: <:<[V, (K2, VX)], pt: PTypeH[T], b: CanParallelTransform[T, To]) = {
    b(prepareSecondarySort(numReducers), secSortMap(f), pt.get(getTypeFamily()))
  }

  def secondarySortAndFlatMap[K2, VX, T, To](f: (K, Iterable[(K2, VX)]) => TraversableOnce[T], numReducers: Int = -1)
    (implicit ev: <:<[V, (K2, VX)], pt: PTypeH[T], b: CanParallelTransform[T, To]) = {
    b(prepareSecondarySort(numReducers), secSortFlatMap(f), pt.get(getTypeFamily()))
  }

  private def prepareSecondarySort[K2, VX](numReducers: Int)(implicit ev: <:<[V, (K2, VX)]): PGroupedTable[CPair[K, K2], (K2, VX)] = {
    val basePTF = getTypeFamily().ptf
    val gopts = GroupingOptions.builder()
      .requireSortedKeys()
      .groupingComparatorClass(JoinUtils.getGroupingComparator(basePTF))
      .partitionerClass(JoinUtils.getPartitionerClass(basePTF))
    if (numReducers > 0) {
      gopts.numReducers(numReducers)
    }
    val kt = basePTF.pairs(keyType(), valueType().getSubTypes.get(0).asInstanceOf[PType[K2]])
    val ptt = basePTF.tableOf(kt, valueType.asInstanceOf[PType[(K2, VX)]])
    parallelDo(new SPrepareSSFn[K, V, K2, VX], ptt).groupByKey(gopts.build())
  }

  def mapValues[T](f: V => T)(implicit pt: PTypeH[T]) = {
    val ptf = getTypeFamily()
    val ptype = ptf.tableOf(native.getKeyType(), pt.get(ptf))
    parallelDo(mapValuesFn[K, V, T](f), ptype)
  }

  def mapValues[T](f: V => T, pt: PType[T]) = {
    val ptype = pt.getFamily().tableOf(native.getKeyType(), pt)
    parallelDo(mapValuesFn[K, V, T](f), ptype)
  }

  def mapKeys[T](f: K => T)(implicit pt: PTypeH[T]) = {
    val ptf = getTypeFamily()
    val ptype = ptf.tableOf(pt.get(ptf), native.getValueType())
    parallelDo(mapKeysFn[K, V, T](f), ptype)
  }

  def mapKeys[T](f: K => T, pt: PType[T]) = {
    val ptype = pt.getFamily.tableOf(pt, native.getValueType())
    parallelDo(mapKeysFn[K, V, T](f), ptype)
  }

  def union(others: PTable[K, V]*) = {
    new PTable[K, V](native.union(others.map(_.native) : _*))
  }

  def keys() = new PCollection[K](PTables.keys(native))

  def values() = new PCollection[V](PTables.values(native))

  def cogroup[V2](other: PTable[K, V2], parallelism: Int = 0) = {
    val jres = Cogroup.cogroup[K, V, V2](parallelism, this.native, other.native)
    val ptf = getTypeFamily()
    val inter = new PTable[K, CPair[JCollect[V], JCollect[V2]]](jres)
    inter.parallelDo(new SMapTableValuesFn[K, CPair[JCollect[V], JCollect[V2]], (Iterable[V], Iterable[V2])] {
      def apply(x: CPair[JCollect[V], JCollect[V2]]) = {
        (collectionAsScalaIterable[V](x.first()), collectionAsScalaIterable[V2](x.second()))
      }
    }, ptf.tableOf(keyType, ptf.tuple2(ptf.collections(valueType), ptf.collections(other.valueType))))
  }

  protected def join[V2](other: PTable[K, V2], joinType: JoinType, parallelism: Int): PTable[K, (V, V2)] = {
    val strategy = new DefaultJoinStrategy[K, V, V2](parallelism)
    val jres = strategy.join(this.native, other.native, joinType)
    val ptf = getTypeFamily()
    val ptype = ptf.tableOf(keyType, ptf.tuple2(valueType, other.valueType))
    val inter = new PTable[K, CPair[V, V2]](jres)
    inter.parallelDo(new SMapTableValuesFn[K, CPair[V, V2], (V, V2)] {
      def apply(x: CPair[V, V2]) = (x.first(), x.second())
    }, ptype)
  }

  def join[V2](other: PTable[K, V2], parallelism: Int = -1): PTable[K, (V, V2)] = {
    innerJoin(other, parallelism)
  }

  def innerJoin[V2](other: PTable[K, V2], parallelism: Int = -1): PTable[K, (V, V2)] = {
    join[V2](other, JoinType.INNER_JOIN, parallelism)
  }

  def leftJoin[V2](other: PTable[K, V2], parallelism: Int = -1): PTable[K, (V, V2)] = {
    join[V2](other, JoinType.LEFT_OUTER_JOIN, parallelism)
  }

  def rightJoin[V2](other: PTable[K, V2], parallelism: Int = -1): PTable[K, (V, V2)] = {
    join[V2](other, JoinType.RIGHT_OUTER_JOIN, parallelism)
  }

  def fullJoin[V2](other: PTable[K, V2], parallelism: Int = -1): PTable[K, (V, V2)] = {
    join[V2](other, JoinType.FULL_OUTER_JOIN, parallelism)
  }

  def joinUsing[V2](other: PTable[K, V2], strategy: ScrunchJoinStrategy[K, V, V2],
                    joinType: JoinType): PTable[K, (V, V2)] = {
     strategy.join(this, other, joinType)
  }

  def joinUsing[V2](other: PTable[K, V2], strategy: ScrunchJoinStrategy[K, V, V2]): PTable[K, (V, V2)] = {
    innerJoinUsing[V2](other, strategy)
  }

  def innerJoinUsing[V2](other: PTable[K, V2], strategy: ScrunchJoinStrategy[K, V, V2]) = {
    joinUsing[V2](other, strategy, JoinType.INNER_JOIN)
  }

  def leftJoinUsing[V2](other: PTable[K, V2], strategy: ScrunchJoinStrategy[K, V, V2]) = {
    joinUsing[V2](other, strategy, JoinType.LEFT_OUTER_JOIN)
  }

  def rightJoinUsing[V2](other: PTable[K, V2], strategy: ScrunchJoinStrategy[K, V, V2]) = {
    joinUsing[V2](other, strategy, JoinType.RIGHT_OUTER_JOIN)
  }

  def fullJoinUsing[V2](other: PTable[K, V2], strategy: ScrunchJoinStrategy[K, V, V2]) = {
    joinUsing[V2](other, strategy, JoinType.FULL_OUTER_JOIN)
  }

  def cross[K2, V2](other: PTable[K2, V2]): PTable[(K, K2), (V, V2)] = {
    val ptf = getTypeFamily()
    val inter = new PTable(Cartesian.cross(this.native, other.native))
    val f = (k: CPair[K,K2], v: CPair[V,V2]) => CPair.of((k.first(), k.second()), (v.first(), v.second()))
    inter.parallelDo(mapFn(f), ptf.tableOf(ptf.tuple2(keyType, other.keyType), ptf.tuple2(valueType, other.valueType)))
  }

  def top(limit: Int, maximize: Boolean) = {
    wrap(Aggregate.top(this.native, limit, maximize))
  }

  def groupByKey() = new PGroupedTable(native.groupByKey())

  def groupByKey(partitions: Int) = new PGroupedTable(native.groupByKey(partitions))

  def groupByKey(options: GroupingOptions) = new PGroupedTable(native.groupByKey(options))

  protected def wrap(newNative: JCollection[_]) = {
    new PTable[K, V](newNative.asInstanceOf[JTable[K, V]])
  }

  def increment(groupName: String, counterName: String, count: Long) = {
    new IncrementPTable[K, V](this).apply(groupName, counterName, count)
  }

  def incrementIf(f: (K, V) => Boolean) = new IncrementIfPTable[K, V](this, incFn(f))

  def incrementIfKey(f: K => Boolean) = new IncrementIfPTable[K, V](this, incKeyFn(f))

  def incrementIfValue(f: V => Boolean) = new IncrementIfPTable[K, V](this, incValueFn(f))

  def materialize(): Iterable[(K, V)] = {
    setupRun()
    native.materialize.view.map(x => (x.first, x.second))
  }

  def materializeToMap(): Map[K, V] = {
    setupRun()
    native.materializeToMap().view.toMap
  }

  def asMap(): PObject[Map[K, V]] = {
    setupRun()
    PObject(native.asMap())
  }

  def asPCollection(): PCollection[(K, V)] = {
    val pType = getTypeFamily().tuple2(native.getKeyType, native.getValueType)
    new PCollection(native.parallelDo(new MapFn[CPair[K, V], (K, V)] {
      override def map(input: CPair[K, V]): (K, V) = (input.first(), input.second())
    }, pType))
  }

  def pType() = native.getPTableType()

  def keyType() = native.getPTableType().getKeyType()

  def valueType() = native.getPTableType().getValueType()
}

trait SDoTableFn[K, V, T] extends DoFn[CPair[K, V], T] with Function2[K, V, TraversableOnce[T]] {
  override def process(input: CPair[K, V], emitter: Emitter[T]) {
    for (v <- apply(input.first(), input.second())) {
      emitter.emit(v)
    }
  }
}

trait SMapTableFn[K, V, T] extends MapFn[CPair[K, V], T] with Function2[K, V, T] {
  override def map(input: CPair[K, V]) = apply(input.first(), input.second())
}

trait SFilterTableFn[K, V] extends FilterFn[CPair[K, V]] with Function2[K, V, Boolean] {
  override def accept(input: CPair[K, V]) = apply(input.first(), input.second())
}

class SDoTableWithCtxtFn[K, V, T](val f: (K, V, TaskInputOutputContext[_, _, _, _]) => TraversableOnce[T])
  extends DoFn[CPair[K, V], T] {
  override def process(input: CPair[K, V], emitter: Emitter[T]) {
    for (v <- f(input.first(), input.second(), getContext)) {
      emitter.emit(v)
    }
  }
}

class SMapTableWithCtxtFn[K, V, T](val f: (K, V, TaskInputOutputContext[_, _, _, _]) => T)
  extends MapFn[CPair[K, V], T] {
  override def map(input: CPair[K, V]) = f(input.first(), input.second(), getContext)
}

class SFilterTableWithCtxtFn[K, V](val f: (K, V, TaskInputOutputContext[_, _, _, _]) => Boolean)
  extends FilterFn[CPair[K, V]] {
  override def accept(input: CPair[K, V]) = f.apply(input.first(), input.second(), getContext)
}

trait SDoPairTableFn[K, V, S, T] extends DoFn[CPair[K, V], CPair[S, T]] with Function2[K, V, TraversableOnce[(S, T)]] {
  override def process(input: CPair[K, V], emitter: Emitter[CPair[S, T]]) {
    for (v <- apply(input.first(), input.second())) {
      emitter.emit(CPair.of(v._1, v._2))
    }
  }
}

trait SMapPairTableFn[K, V, S, T] extends MapFn[CPair[K, V], CPair[S, T]] with Function2[K, V, (S, T)] {
  override def map(input: CPair[K, V]) = {
    val t = apply(input.first(), input.second())
    CPair.of(t._1, t._2)
  }
}

trait SMapTableValuesFn[K, V, T] extends MapFn[CPair[K, V], CPair[K, T]] with Function1[V, T] {
  override def map(input: CPair[K, V]) = CPair.of(input.first(), apply(input.second()))
}

trait SMapTableKeysFn[K, V, T] extends MapFn[CPair[K, V], CPair[T, V]] with Function1[K, T] {
  override def map(input: CPair[K, V]) = CPair.of(apply(input.first()), input.second())
}

private class SPrepareSSFn[K, V, K2, VX] extends MapFn[CPair[K, V], CPair[CPair[K, K2], (K2, VX)]] {
  override def map(input: CPair[K, V]): CPair[CPair[K, K2], (K2, VX)] = {
    val sec = input.second().asInstanceOf[(K2, VX)]
    CPair.of(CPair.of(input.first(), sec._1), sec)
  }
}

trait SecSortFlatMapFn[K, K2, VX, T] extends DoFn[CPair[CPair[K, K2], JIterable[(K2, VX)]], T]
  with Function2[K, Iterable[(K2, VX)], TraversableOnce[T]] {
  override def process(input: CPair[CPair[K, K2], JIterable[(K2, VX)]], emitter: Emitter[T]) {
    val iter = iterableAsScalaIterable(input.second())
    for (v <- apply(input.first().first(), iter)) {
      emitter.emit(v)
    }
  }
}

trait SecSortMapFn[K, K2, VX, T] extends MapFn[CPair[CPair[K, K2], JIterable[(K2, VX)]], T]
  with Function2[K, Iterable[(K2, VX)], T] {
  override def map(input: CPair[CPair[K, K2], JIterable[(K2, VX)]]) = {
    apply(input.first().first(), iterableAsScalaIterable(input.second()))
  }
}

object PTable {
  type TIOC = TaskInputOutputContext[_, _, _, _]

  def flatMapFn[K, V, T](fn: (K, V) => TraversableOnce[T]) = {
    new SDoTableFn[K, V, T] { def apply(k: K, v: V) = fn(k, v) }
  }

  def mapFn[K, V, T](fn: (K, V) => T) = {
    new SMapTableFn[K, V, T] { def apply(k: K, v: V) = fn(k, v) }
  }

  def filterFn[K, V](fn: (K, V) => Boolean) = {
    new SFilterTableFn[K, V] { def apply(k: K, v: V) = fn(k, v) }
  }

  def flatMapWithCtxtFn[K, V, T](fn: (K, V, TIOC) => TraversableOnce[T]) = {
    new SDoTableWithCtxtFn[K, V, T](fn)
  }

  def mapWithCtxtFn[K, V, T](fn: (K, V, TIOC) => T) = {
    new SMapTableWithCtxtFn[K, V, T](fn)
  }

  def filterWithCtxtFn[K, V](fn: (K, V, TIOC) => Boolean) = {
    new SFilterTableWithCtxtFn[K, V](fn)
  }

  def mapValuesFn[K, V, T](fn: V => T) = {
    new SMapTableValuesFn[K, V, T] { def apply(v: V) = fn(v) }
  }

  def mapKeysFn[K, V, T](fn: K => T) = {
    new SMapTableKeysFn[K, V, T] { def apply(k: K) = fn(k) }
  }

  def pairMapFn[K, V, S, T](fn: (K, V) => (S, T)) = {
    new SMapPairTableFn[K, V, S, T] { def apply(k: K, v: V) = fn(k, v) }
  }

  def pairFlatMapFn[K, V, S, T](fn: (K, V) => TraversableOnce[(S, T)]) = {
    new SDoPairTableFn[K, V, S, T] { def apply(k: K, v: V) = fn(k, v) }
  }

  def incFn[K, V, T](fn: (K, V) => T) = new Function1[CPair[K, V], T] with Serializable {
    def apply(p: CPair[K, V]): T = fn(p.first(), p.second())
  }

  def incKeyFn[K, V, T](fn: K => T) = new Function1[CPair[K, V], T] with Serializable {
    def apply(p: CPair[K, V]): T = fn(p.first())
  }

  def incValueFn[K, V, T](fn: V => T) = new Function1[CPair[K, V], T] with Serializable {
    def apply(p: CPair[K, V]): T = fn(p.second())
  }

  def secSortFlatMap[K, K2, VX, T](fn: (K, Iterable[(K2, VX)]) => TraversableOnce[T]) = {
    new SecSortFlatMapFn[K, K2, VX, T] {
      def apply(k: K, v: Iterable[(K2, VX)]) = fn(k, v)
    }
  }

  def secSortMap[K, K2, VX, T](fn: (K, Iterable[(K2, VX)]) => T) = new SecSortMapFn[K, K2, VX, T] {
    def apply(k: K, v: Iterable[(K2, VX)]) = fn(k, v)
  }
}
