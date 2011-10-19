package com.cloudera.scrunch

import com.cloudera.crunch.{DoFn, Emitter, FilterFn, MapFn}
import com.cloudera.crunch.{CombineFn, PGroupedTable => JGroupedTable, PTable => JTable, Pair => JPair}
import com.cloudera.scrunch.Conversions._
import java.lang.{Iterable => JIterable}
import scala.collection.{Iterable, Iterator}


class PGroupedTable[K, V](grouped: JGroupedTable[K, V]) extends PCollection[JPair[K, JIterable[V]]](grouped) with JGroupedTable[K, V] {

  def filter(f: (K, Iterable[V]) => Boolean) = {
    ClosureCleaner.clean(f)
    parallelDo(new DSFilterGroupedFn[K, V](f), grouped.getPType())
  }

  def map[T: ClassManifest](f: (K, Iterable[V]) => T) = {
    ClosureCleaner.clean(f)
    parallelDo(new DSMapGroupedFn[K, V, T](f), createPType(classManifest[T]))
  }

  def map2[L: ClassManifest, W: ClassManifest](f: (K, Iterable[V]) => (L, W)) = {
    ClosureCleaner.clean(f)
    parallelDo(new DSMapGroupedFn2[K, V, L, W](f), createPTableType(classManifest[L], classManifest[W]))
  }

  def flatMap[T: ClassManifest](f: (K, Iterable[V]) => Traversable[T]) = {
    ClosureCleaner.clean(f)
    parallelDo(new DSDoGroupedFn[K, V, T](f), createPType(classManifest[T]))
  }

  def flatMap2[L: ClassManifest, W: ClassManifest](f: (K, Iterable[V]) => Traversable[(L, W)]) = {
    ClosureCleaner.clean(f)
    parallelDo(new DSDoGroupedFn2[K, V, L, W](f), createPTableType(classManifest[L], classManifest[W]))
  }

  def combine(f: Iterable[V] => V) = combineValues(new IterableCombineFn[K, V](f))

  override def combineValues(fn: CombineFn[K, V]) = new PTable[K, V](grouped.combineValues(fn))

  override def ungroup() = new PTable[K, V](grouped.ungroup())
}

class IterableCombineFn[K, V](f: Iterable[V] => V) extends CombineFn[K, V] {
  ClosureCleaner.clean(f)
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

trait SFilterGroupedFn[K, V] extends FilterFn[JPair[K, JIterable[V]]] with Function2[K, Iterable[V], Boolean] {
  override def accept(input: JPair[K, JIterable[V]]): Boolean = {
    apply(c2s(input.first()).asInstanceOf[K], new ConversionIterable[V](input.second()))
  }
}

trait SDoGroupedFn[K, V, T] extends DoFn[JPair[K, JIterable[V]], T] with Function2[K, Iterable[V], Traversable[T]] {
  override def process(input: JPair[K, JIterable[V]], emitter: Emitter[T]): Unit = {
    for (v <- apply(c2s(input.first()).asInstanceOf[K], new ConversionIterable[V](input.second()))) {
      emitter.emit(s2c(v).asInstanceOf[T])
    }
  }
}

trait SDoGroupedFn2[K, V, L, W] extends DoFn[JPair[K, JIterable[V]], JPair[L, W]]
    with Function2[K, Iterable[V], Traversable[(L, W)]] {
  override def process(input: JPair[K, JIterable[V]], emitter: Emitter[JPair[L, W]]): Unit = {
    for ((f, s) <- apply(c2s(input.first()).asInstanceOf[K], new ConversionIterable[V](input.second()))) {
      emitter.emit(JPair.of(s2c(f).asInstanceOf[L], s2c(s).asInstanceOf[W]))
    }
  }
}

trait SMapGroupedFn[K, V, T] extends MapFn[JPair[K, JIterable[V]], T] with Function2[K, Iterable[V], T] {
  override def map(input: JPair[K, JIterable[V]]): T = {
    s2c(apply(c2s(input.first()).asInstanceOf[K], new ConversionIterable[V](input.second()))).asInstanceOf[T]
  }
}

trait SMapGroupedFn2[K, V, L, W] extends MapFn[JPair[K, JIterable[V]], JPair[L, W]] with Function2[K, Iterable[V], (L, W)]{
  override def map(input: JPair[K, JIterable[V]]): JPair[L, W] = {
    val (f, s) = apply(c2s(input.first()).asInstanceOf[K], new ConversionIterable[V](input.second()))
    JPair.of(s2c(f).asInstanceOf[L], s2c(s).asInstanceOf[W])
  }
}

class DSFilterGroupedFn[K, V](fn: (K, Iterable[V]) => Boolean) extends SFilterGroupedFn[K, V] {
  ClosureCleaner.clean(fn)
  def apply(k: K, v: Iterable[V]) = fn(k, v)    
}

class DSDoGroupedFn[K, V, T](fn: (K, Iterable[V]) => Traversable[T]) extends SDoGroupedFn[K, V, T] {
  ClosureCleaner.clean(fn)
  def apply(k: K, v: Iterable[V]) = fn(k, v)    
}

class DSDoGroupedFn2[K, V, L, W](fn: (K, Iterable[V]) => Traversable[(L, W)]) extends SDoGroupedFn2[K, V, L, W] {
  ClosureCleaner.clean(fn)
  def apply(k: K, v: Iterable[V]) = fn(k, v)  
}

class DSMapGroupedFn[K, V, T](fn: (K, Iterable[V]) => T) extends SMapGroupedFn[K, V, T] {
  ClosureCleaner.clean(fn)
  def apply(k: K, v: Iterable[V]) = fn(k, v)  
}

class DSMapGroupedFn2[K, V, L, W](fn: (K, Iterable[V]) => (L, W)) extends SMapGroupedFn2[K, V, L, W] {
  ClosureCleaner.clean(fn)
  def apply(k: K, v: Iterable[V]) = fn(k, v)
}
