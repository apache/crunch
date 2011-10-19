package com.cloudera.scrunch

import com.cloudera.crunch.{DoFn, Emitter, FilterFn, MapFn}
import com.cloudera.crunch.{CombineFn, PGroupedTable => JGroupedTable, PTable => JTable, Pair => JPair}
import java.lang.{Iterable => JIterable}
import scala.collection.{Iterable, Iterator}

import Conversions._

class PGroupedTable[K, V](grouped: JGroupedTable[K, V]) extends PCollection[JPair[K, JIterable[V]]](grouped) with JGroupedTable[K, V] {

  def filter(f: (K, Iterable[V]) => Boolean) = {
    ClosureCleaner.clean(f)
    parallelDo(new SGroupedTableFilterFn[K, V](f), grouped.getPType())
  }

  def map[T: ClassManifest](f: (K, Iterable[V]) => T) = {
    ClosureCleaner.clean(f)
    parallelDo(new SGroupedTableMapFn[K, V, T](f), createPType(classManifest[T]))
  }

  def map2[L: ClassManifest, W: ClassManifest](f: (K, Iterable[V]) => (L, W)) = {
    ClosureCleaner.clean(f)
    parallelDo(new SGroupedTableMapTableFn[K, V, L, W](f), createPTableType(classManifest[L], classManifest[W]))
  }

  def flatMap[T: ClassManifest](f: (K, Iterable[V]) => Traversable[T]) = {
    ClosureCleaner.clean(f)
    parallelDo(new SGroupedTableDoFn[K, V, T](f), createPType(classManifest[T]))
  }

  def flatMap2[L: ClassManifest, W: ClassManifest](f: (K, Iterable[V]) => Traversable[(L, W)]) = {
    ClosureCleaner.clean(f)
    parallelDo(new SGroupedTableDoTableFn[K, V, L, W](f), createPTableType(classManifest[L], classManifest[W]))
  }

  def combine(f: Iterable[V] => V) = combineValues(new IterableCombineFn[K, V](f))

  override def combineValues(fn: CombineFn[K, V]) = new PTable[K, V](grouped.combineValues(fn))

  override def ungroup() = new PTable[K, V](grouped.ungroup())
}

class IterableCombineFn[K, V](f: Iterable[V] => V) extends CombineFn[K, V] {
  override def combine(v: JIterable[V]) = {
    s2c(f(new ConversionIterable[V](v))).asInstanceOf[V]
  }
}

class ConversionIterator[S](iterator: java.util.Iterator[S]) extends Iterator[S] {
  override def hasNext() = iterator.hasNext()
  override def next() = c2s(iterator.next()).asInstanceOf[S]
}

class ConversionIterable[S](iterable: JIterable[S]) extends Iterable[S] {
  override def iterator() = new ConversionIterator[S](iterable.iterator())
}

class SGroupedTableFilterFn[K, V](f: (K, Iterable[V]) => Boolean) extends FilterFn[JPair[K, JIterable[V]]] {
  override def accept(input: JPair[K, JIterable[V]]): Boolean = {
    f(Conversions.c2s(input.first()).asInstanceOf[K], new ConversionIterable[V](input.second()))
  }
}

class SGroupedTableDoFn[K, V, T](fn: (K, Iterable[V]) => Traversable[T]) extends DoFn[JPair[K, JIterable[V]], T] {
  override def process(input: JPair[K, JIterable[V]], emitter: Emitter[T]): Unit = {
    for (v <- fn(Conversions.c2s(input.first()).asInstanceOf[K], new ConversionIterable[V](input.second()))) {
      emitter.emit(Conversions.s2c(v).asInstanceOf[T])
    }
  }
}

class SGroupedTableDoTableFn[K, V, L, W](fn: (K, Iterable[V]) => Traversable[(L, W)]) extends DoFn[JPair[K, JIterable[V]], JPair[L, W]] {
  override def process(input: JPair[K, JIterable[V]], emitter: Emitter[JPair[L, W]]): Unit = {
    for ((f, s) <- fn(Conversions.c2s(input.first()).asInstanceOf[K], new ConversionIterable[V](input.second()))) {
      emitter.emit(JPair.of(Conversions.s2c(f).asInstanceOf[L], Conversions.s2c(s).asInstanceOf[W]))
    }
  }
}

class SGroupedTableMapFn[K, V, T](fn: (K, Iterable[V]) => T) extends MapFn[JPair[K, JIterable[V]], T] {
  override def map(input: JPair[K, JIterable[V]]): T = {
    Conversions.s2c(fn(Conversions.c2s(input.first()).asInstanceOf[K], new ConversionIterable[V](input.second()))).asInstanceOf[T]
  }
}

class SGroupedTableMapTableFn[K, V, L, W](fn: (K, Iterable[V]) => (L, W)) extends MapFn[JPair[K, JIterable[V]], JPair[L, W]] {
  override def map(input: JPair[K, JIterable[V]]): JPair[L, W] = {
    val (f, s) = fn(Conversions.c2s(input.first()).asInstanceOf[K], new ConversionIterable[V](input.second()))
    JPair.of(Conversions.s2c(f).asInstanceOf[L], Conversions.s2c(s).asInstanceOf[W])
  }
}
