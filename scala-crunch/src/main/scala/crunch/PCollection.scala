package crunch

import com.cloudera.crunch.{DoFn, Emitter, FilterFn, MapFn}
import com.cloudera.crunch.{PCollection => JCollection, PTable => JTable, Pair => JPair, Target}
import com.cloudera.crunch.`type`.{PType, PTableType}

class PCollection[S](jcollect: JCollection[S]) extends JCollection[S] {

  def filter(f: Any => Boolean): PCollection[S] = {
    ClosureCleaner.clean(f)
    parallelDo(new SFilterFn[S](f), getPType())
  }

  def map[T: ClassManifest](f: Any => T) = {
    ClosureCleaner.clean(f)
    parallelDo(new SMapFn[S, T](f), getPType(classManifest[T]))
  }

  def map[K: ClassManifest, V: ClassManifest](f: Any => (K, V)) = {
    val ptf = getTypeFamily()
    val keyType = getPType(classManifest[K])
    val valueType = getPType(classManifest[V])
    ClosureCleaner.clean(f)
    parallelDo(new SMapTableFn[S, K, V](f), ptf.tableOf(keyType, valueType))
  }

  def flatMap[T: ClassManifest](f: Any => Seq[T]) = {
    ClosureCleaner.clean(f)
    parallelDo(new SDoFn[S, T](f), getPType(classManifest[T]))
  }

  def flatMap[K: ClassManifest, V: ClassManifest](f: Any => Seq[(K, V)]) = {
    val ptf = getTypeFamily()
    val keyType = getPType(classManifest[K])
    val valueType = getPType(classManifest[V])
    ClosureCleaner.clean(f)
    parallelDo(new SDoTableFn[S, K, V](f), ptf.tableOf(keyType, valueType))
  }

  protected def getPType[T](m: ClassManifest[T]): PType[T] = {
    Conversions.getPType(m, getTypeFamily()).asInstanceOf[PType[T]]
  }

  override def getPipeline() = jcollect.getPipeline()

  override def union(others: JCollection[S]*) = {
    new PCollection[S](jcollect.union(others.map(baseCheck): _*))
  }

  def base = jcollect

  private def baseCheck(c: JCollection[S]): JCollection[S] = c match {
    case x: PCollection[S] => x.base
    case _ => c
  } 

  def ++ (other: JCollection[S]) = union(other)

  override def parallelDo[T](fn: DoFn[S,T], ptype: PType[T]) = {
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

  override def writeTo(target: Target) = jcollect.writeTo(target)

  override def getPType() = jcollect.getPType()

  override def getTypeFamily() = jcollect.getTypeFamily()

  override def getSize() = jcollect.getSize()

  override def getName() = jcollect.getName()
}

class SDoFn[S, T](fn: Any => Seq[T]) extends DoFn[S, T] {
  override def process(input: S, emitter: Emitter[T]): Unit = {
    for (v <- fn(Conversions.c2s(input))) {
      emitter.emit(Conversions.s2c(v).asInstanceOf[T])
    }
  }
}

class SDoTableFn[S, K, V](fn: Any => Seq[(K, V)]) extends DoFn[S, JPair[K, V]] {
  override def process(input: S, emitter: Emitter[JPair[K, V]]): Unit = {
    for (v <- fn(Conversions.c2s(input))) {
      emitter.emit(Conversions.s2c(v).asInstanceOf[JPair[K, V]])
    }
  }
}

class SFilterFn[T](f: Any => Boolean) extends FilterFn[T] {
  override def accept(input: T): Boolean = {
    f(Conversions.c2s(input));
  }
}

class SMapFn[S, T](fn: Any => T) extends MapFn[S, T] {
  override def map(input: S): T = {
    Conversions.s2c(fn(Conversions.c2s(input))).asInstanceOf[T]
  }
}

class SMapTableFn[S, K, V](fn: Any => (K, V)) extends MapFn[S, JPair[K, V]] {
  override def map(input: S): JPair[K, V] = {
    Conversions.s2c(fn(Conversions.c2s(input))).asInstanceOf[JPair[K, V]]
  }
}

