package crunch

import crunch.fn._
import com.cloudera.crunch.{CombineFn, PGroupedTable => JGroupedTable, PTable => JTable, Pair => JPair}
import java.lang.{Iterable => JIterable}
import scala.collection.Iterable

class PGroupedTable[K, V](grouped: JGroupedTable[K, V]) extends PCollection[JPair[K, JIterable[V]]](grouped) with JGroupedTable[K, V] {

  def filter(f: (Any, Iterable[Any]) => Boolean): PTable[K, V] = {
    ClosureCleaner.clean(f)
    parallelDo(new SFilterGroupedTableFn[K, V](f), getPTableType())
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
