package crunch

import com.cloudera.crunch.{DoFn, Emitter, FilterFn, MapFn}
import com.cloudera.crunch.{PCollection => JCollection, PTable => JTable, Pair => JPair, Target}
import com.cloudera.crunch.`type`.{PType, PTableType}

class PCollection[S](jcollect: JCollection[S]) extends JCollection[S] {

  def filter(f: S => Boolean): PCollection[S] = {
    ClosureCleaner.clean(f)
    parallelDo(new SFilterFn[S](f), getPType())
  }

  def map[T: ClassManifest](f: S => T) = {
    ClosureCleaner.clean(f)
    parallelDo(new SMapFn[S, T](f), createPType(classManifest[T]))
  }

  def map2[K: ClassManifest, V: ClassManifest](f: S => (K, V)) = {
    val ptf = getTypeFamily()
    val keyType = createPType(classManifest[K])
    val valueType = createPType(classManifest[V])
    println(keyType + " " + valueType)
    ClosureCleaner.clean(f)
    parallelDo(new SMapTableFn[S, K, V](f), ptf.tableOf(keyType, valueType))
  }

  def flatMap[T: ClassManifest](f: S => Traversable[T]) = {
    ClosureCleaner.clean(f)
    parallelDo(new SDoFn[S, T](f), createPType(classManifest[T]))
  }

  def flatMap2[K: ClassManifest, V: ClassManifest](f: S => Traversable[(K, V)]) = {
    val ptf = getTypeFamily()
    val keyType = createPType(classManifest[K])
    val valueType = createPType(classManifest[V])
    ClosureCleaner.clean(f)
    parallelDo(new SDoTableFn[S, K, V](f), ptf.tableOf(keyType, valueType))
  }

  protected def createPType[T](m: ClassManifest[T]): PType[T] = {
    Conversions.toPType(m, getTypeFamily()).asInstanceOf[PType[T]]
  }

  override def getPipeline() = jcollect.getPipeline()

  override def union(others: JCollection[S]*) = {
    new PCollection[S](jcollect.union(others.map(baseCheck): _*))
  }

  def base: JCollection[S] = jcollect match {
    case x: PCollection[S] => x.base
    case _ => jcollect
  }

  def baseCheck(collect: JCollection[S]) = collect match {
    case x: PCollection[S] => x.base
    case _ => collect
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

object PCollection {
  implicit def jcollect2pcollect[S](jcollect: JCollection[S]) = new PCollection[S](jcollect)
}

class SDoFn[S, T](fn: S => Traversable[T]) extends DoFn[S, T] {
  override def process(input: S, emitter: Emitter[T]): Unit = {
    for (v <- fn(Conversions.c2s(input).asInstanceOf[S])) {
      emitter.emit(Conversions.s2c(v).asInstanceOf[T])
    }
  }
}

class SDoTableFn[S, K, V](fn: S => Traversable[(K, V)]) extends DoFn[S, JPair[K, V]] {
  override def process(input: S, emitter: Emitter[JPair[K, V]]): Unit = {
    for (v <- fn(Conversions.c2s(input).asInstanceOf[S])) {
      emitter.emit(Conversions.s2c(v).asInstanceOf[JPair[K, V]])
    }
  }
}

class SFilterFn[T](f: T => Boolean) extends FilterFn[T] {
  override def accept(input: T): Boolean = {
    f(Conversions.c2s(input).asInstanceOf[T]);
  }
}

class SMapFn[S, T](fn: S => T) extends MapFn[S, T] {
  override def map(input: S): T = {
    Conversions.s2c(fn(Conversions.c2s(input).asInstanceOf[S])).asInstanceOf[T]
  }
}

class SMapTableFn[S, K, V](fn: S => (K, V)) extends MapFn[S, JPair[K, V]] {
  override def map(input: S): JPair[K, V] = {
    Conversions.s2c(fn(Conversions.c2s(input).asInstanceOf[S])).asInstanceOf[JPair[K, V]]
  }
}

