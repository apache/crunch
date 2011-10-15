package crunch

import com.cloudera.crunch.{DoFn, PCollection => JCollection, PTable => JTable, Pair => JPair, Target}
import com.cloudera.crunch.`type`.{PType, PTableType}

class PCollection[S](jcollect: JCollection[S]) extends JCollection[S] {

  def filter(f: Any => Boolean): PCollection[S] = {
    parallelDo(new SFilterFn[S](f), getPType())
  }

  def map[T: ClassManifest](f: Any => T): PCollection[T] = {
    parallelDo(new SMapFn[S, T](f), getPType(classManifest[T]))
  }

  def map[K: ClassManifest, V: ClassManifest](f: Any => (K, V)): PTable[K, V] = {
    val ptf = getTypeFamily()
    val keyType = getPType(classManifest[K])
    val valueType = getPType(classManifest[V])
    parallelDo(new SMapTableFn[S, K, V](f), ptf.tableOf(keyType, valueType))
  }

  def flatMap[T: ClassManifest](f: Any => Seq[T]): PCollection[T] = {
    parallelDo(new SDoFn[S, T](f), getPType(classManifest[T]))
  }

  def flatMap[K: ClassManifest, V: ClassManifest](f: Any => Seq[(K, V)]): PTable[K, V] = {
    val ptf = getTypeFamily()
    val keyType = getPType(classManifest[K])
    val valueType = getPType(classManifest[V])
    parallelDo(new SDoTableFn[S, K, V](f), ptf.tableOf(keyType, valueType))
  }

  private def getPType[T](m: ClassManifest[T]): PType[T] = {
    Conversions.getPType(m, getTypeFamily()).asInstanceOf[PType[T]]
  }

  override def getPipeline() = jcollect.getPipeline()

  override def union(others: JCollection[S]*) = {
    new PCollection[S](jcollect.union(others.map(baseCheck): _*))
  }

  private def base = jcollect

  private def baseCheck(c: JCollection[S]): JCollection[S] = c match {
    case x: PCollection[S] => x.base
    case _ => c
  } 

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
