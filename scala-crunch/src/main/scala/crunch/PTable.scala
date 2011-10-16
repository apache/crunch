package crunch

import com.cloudera.crunch.{DoFn, Emitter, FilterFn, MapFn}
import com.cloudera.crunch.{GroupingOptions, PTable => JTable, Pair => JPair}

class PTable[K, V](jtable: JTable[K, V]) extends PCollection[JPair[K, V]](jtable) with JTable[K, V] {

  override def getPTableType() = jtable.getPTableType()

  override def getKeyType() = jtable.getKeyType()

  override def getValueType() = jtable.getValueType()

  def filter(f: (Any, Any) => Boolean): PTable[K, V] = {
    ClosureCleaner.clean(f)
    parallelDo(new STableFilterFn[K, V](f), getPTableType())
  }

  def map[T: ClassManifest](f: (Any, Any) => T) = {
    ClosureCleaner.clean(f)
    parallelDo(new STableMapFn[K, V, T](f), getPType(classManifest[T]))
  }

  def map2[L: ClassManifest, W: ClassManifest](f: (Any, Any) => (L, W)) = {
    val ptf = getTypeFamily()
    val keyType = getPType(classManifest[L])
    val valueType = getPType(classManifest[W])
    ClosureCleaner.clean(f)
    parallelDo(new STableMapTableFn[K, V, L, W](f), ptf.tableOf(keyType, valueType))
  }

  def flatMap[T: ClassManifest](f: (Any, Any) => Traversable[T]) = {
    ClosureCleaner.clean(f)
    parallelDo(new STableDoFn[K, V, T](f), getPType(classManifest[T]))
  }

  def flatMap2[L: ClassManifest, W: ClassManifest](f: (Any, Any) => Traversable[(L, W)]) = {
    val ptf = getTypeFamily()
    val keyType = getPType(classManifest[L])
    val valueType = getPType(classManifest[W])
    ClosureCleaner.clean(f)
    parallelDo(new STableDoTableFn[K, V, L, W](f), ptf.tableOf(keyType, valueType))
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
}

class STableFilterFn[K, V](f: (Any, Any) => Boolean) extends FilterFn[JPair[K, V]] {
  override def accept(input: JPair[K, V]): Boolean = {
    f(Conversions.c2s(input.first()), Conversions.c2s(input.second()));
  }
}

class STableDoFn[K, V, T](fn: (Any, Any) => Traversable[T]) extends DoFn[JPair[K, V], T] {
  override def process(input: JPair[K, V], emitter: Emitter[T]): Unit = {
    for (v <- fn(Conversions.c2s(input.first()), Conversions.c2s(input.second()))) {
      emitter.emit(Conversions.s2c(v).asInstanceOf[T])
    }
  }
}

class STableDoTableFn[K, V, L, W](fn: (Any, Any) => Traversable[(Any, Any)]) extends DoFn[JPair[K, V], JPair[L, W]] {
  override def process(input: JPair[K, V], emitter: Emitter[JPair[L, W]]): Unit = {
    for ((f, s) <- fn(Conversions.c2s(input.first()), Conversions.c2s(input.second()))) {
      emitter.emit(JPair.of(Conversions.s2c(f), Conversions.s2c(s)).asInstanceOf[JPair[L, W]])
    }
  }
}

class STableMapFn[K, V, T](fn: (Any, Any) => T) extends MapFn[JPair[K, V], T] {
  override def map(input: JPair[K, V]): T = {
    val v = fn(Conversions.c2s(input.first()), Conversions.c2s(input.second()))
    Conversions.s2c(v).asInstanceOf[T]
  }
}

class STableMapTableFn[K, V, L, W](fn: (Any, Any) => (Any, Any)) extends MapFn[JPair[K, V], JPair[L, W]] {
  override def map(input: JPair[K, V]): JPair[L, W] = {
    val (f, s) = fn(Conversions.c2s(input.first()), Conversions.c2s(input.second()))
    JPair.of(Conversions.s2c(f), Conversions.s2c(s)).asInstanceOf[JPair[L, W]]
  }
}
