package crunch

import com.cloudera.crunch.{DoFn, Emitter, FilterFn, MapFn}
import com.cloudera.crunch.{CombineFn, PGroupedTable => JGroupedTable, PTable => JTable, Pair => JPair}
import java.lang.{Iterable => JIterable}
import scala.collection.{Iterable, Iterator}

class PGroupedTable[K, V](grouped: JGroupedTable[K, V]) extends PCollection[JPair[K, JIterable[V]]](grouped) with JGroupedTable[K, V] {

  def filter(f: (Any, Iterable[Any]) => Boolean) = {
    ClosureCleaner.clean(f)
    parallelDo(new SGroupedTableFilterFn[K, V](f), grouped.getPType())
  }

  def map[T: ClassManifest](f: (Any, Iterable[Any]) => T) = {
    ClosureCleaner.clean(f)
    parallelDo(new SGroupedTableMapFn[K, V, T](f), getPType(classManifest[T]))
  }

  def map[L: ClassManifest, W: ClassManifest](f: (Any, Iterable[Any]) => (L, W)) = {
    val ptf = getTypeFamily()
    val keyType = getPType(classManifest[L])
    val valueType = getPType(classManifest[W])
    ClosureCleaner.clean(f)
    parallelDo(new SGroupedTableMapTableFn[K, V, L, W](f), ptf.tableOf(keyType, valueType))
  }

  def flatMap[T: ClassManifest](f: (Any, Iterable[Any]) => Seq[T]) = {
    ClosureCleaner.clean(f)
    parallelDo(new SGroupedTableDoFn[K, V, T](f), getPType(classManifest[T]))
  }

  def flatMap[L: ClassManifest, W: ClassManifest](f: (Any, Iterable[Any]) => Seq[(L, W)]) = {
    val ptf = getTypeFamily()
    val keyType = getPType(classManifest[L])
    val valueType = getPType(classManifest[W])
    ClosureCleaner.clean(f)
    parallelDo(new SGroupedTableDoTableFn[K, V, L, W](f), ptf.tableOf(keyType, valueType))
  }

  override def combineValues(fn: CombineFn[K, V]) = new PTable[K, V](grouped.combineValues(fn))

  override def ungroup() = new PTable[K, V](grouped.ungroup())
}

class ConversionIterator[S](iterator: java.util.Iterator[S]) extends Iterator[S] {
  override def hasNext(): Boolean = iterator.hasNext()

  override def next(): S = Conversions.c2s(iterator.next()).asInstanceOf[S]
}

class ConversionIterable[S](iterable: JIterable[S]) extends Iterable[S] {
  override def iterator() = new ConversionIterator[S](iterable.iterator())
}

class SGroupedTableFilterFn[K, V](f: (Any, Iterable[Any]) => Boolean) extends FilterFn[JPair[K, JIterable[V]]] {
  override def accept(input: JPair[K, JIterable[V]]): Boolean = {
    f(Conversions.c2s(input.first()), new ConversionIterable[V](input.second()))
  }
}

class SGroupedTableDoFn[K, V, T](fn: (Any, Iterable[Any]) => Seq[T]) extends DoFn[JPair[K, JIterable[V]], T] {
  override def process(input: JPair[K, JIterable[V]], emitter: Emitter[T]): Unit = {
    for (v <- fn(Conversions.c2s(input.first()), new ConversionIterable[V](input.second()))) {
      emitter.emit(Conversions.s2c(v).asInstanceOf[T])
    }
  }
}

class SGroupedTableDoTableFn[K, V, L, W](fn: (Any, Iterable[Any]) => Seq[(Any, Any)]) extends DoFn[JPair[K, JIterable[V]], JPair[L, W]] {
  override def process(input: JPair[K, JIterable[V]], emitter: Emitter[JPair[L, W]]): Unit = {
    for ((f, s) <- fn(Conversions.c2s(input.first()), new ConversionIterable[V](input.second()))) {
      emitter.emit(JPair.of(Conversions.s2c(f).asInstanceOf[L], Conversions.s2c(s).asInstanceOf[W]))
    }
  }
}

class SGroupedTableMapFn[K, V, T](fn: (Any, Iterable[Any]) => T) extends MapFn[JPair[K, JIterable[V]], T] {
  override def map(input: JPair[K, JIterable[V]]): T = {
    Conversions.s2c(fn(Conversions.c2s(input.first()), new ConversionIterable[V](input.second()))).asInstanceOf[T]
  }
}

class SGroupedTableMapTableFn[K, V, L, W](fn: (Any, Iterable[Any]) => (Any, Any)) extends MapFn[JPair[K, JIterable[V]], JPair[L, W]] {
  override def map(input: JPair[K, JIterable[V]]): JPair[L, W] = {
    val (f, s) = fn(Conversions.c2s(input.first()), new ConversionIterable[V](input.second()))
    JPair.of(Conversions.s2c(f).asInstanceOf[L], Conversions.s2c(s).asInstanceOf[W])
  }
}
