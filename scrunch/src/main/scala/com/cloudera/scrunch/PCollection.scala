package com.cloudera.scrunch

import com.cloudera.crunch.{DoFn, Emitter, FilterFn, MapFn}
import com.cloudera.crunch.{PCollection => JCollection, PTable => JTable, Pair => JPair, Target}
import com.cloudera.crunch.fn.IdentityFn
import com.cloudera.crunch.`type`.{PType, PTableType, PTypeFamily}
import com.cloudera.scrunch.Conversions._

class PCollection[S](jcollect: JCollection[S]) extends JCollection[S] {

  def using(typeFamily: PTypeFamily) = {
    parallelDo(IdentityFn.getInstance[S](), typeFamily.as(getPType()))
  }
  
  def filter(f: S => Boolean): PCollection[S] = {
    parallelDo(new DSFilterFn[S](f), getPType())
  }

  def map[T: ClassManifest](f: S => T) = {
    parallelDo(new DSMapFn[S, T](f), createPType(classManifest[T]))
  }

  def map2[K: ClassManifest, V: ClassManifest](f: S => (K, V)) = {
    parallelDo(new DSMapFn2[S, K, V](f), createPTableType(classManifest[K], classManifest[V]))
  }

  def flatMap[T: ClassManifest](f: S => Traversable[T]) = {
    parallelDo(new DSDoFn[S, T](f), createPType(classManifest[T]))
  }

  def flatMap2[K: ClassManifest, V: ClassManifest](f: S => Traversable[(K, V)]) = {
    parallelDo(new DSDoFn2[S, K, V](f), createPTableType(classManifest[K], classManifest[V]))
  }

  def groupBy[K: ClassManifest](f: S => K): PGroupedTable[K, S] = {
    val ptype = getTypeFamily().tableOf(createPType(classManifest[K]), getPType())
    parallelDo(new DSMapKeyFn[S, K](f), ptype).groupByKey()
  }

  def apply[T: ClassManifest](doFn: DoFn[S, T]) = {
    parallelDo(doFn, createPType(classManifest[T])) 
  }

  def apply[T: ClassManifest](name: String, doFn: DoFn[S, T]) = {
    parallelDo(name, doFn, createPType(classManifest[T]))
  }

  def apply2[K: ClassManifest, V: ClassManifest](doFn: DoFn[S, JPair[K, V]]) = {
    parallelDo(doFn, createPTableType(classManifest[K], classManifest[V])) 
  }

  def apply2[K: ClassManifest, V: ClassManifest](name: String, doFn: DoFn[S, JPair[K, V]]) = {
    parallelDo(name, doFn, createPTableType(classManifest[K], classManifest[V]))
  }

  protected def createPTableType[K, V](k: ClassManifest[K], v: ClassManifest[V]) = {
    getTypeFamily().tableOf(createPType(k), createPType(v))
  }

  protected def createPType[T](m: ClassManifest[T]): PType[T] = {
    manifest2PType(m, getTypeFamily()).asInstanceOf[PType[T]]
  }

  override def getPipeline() = jcollect.getPipeline()

  override def union(others: JCollection[S]*) = {
    new PCollection[S](jcollect.union(others.map(baseCheck): _*))
  }

  def base: JCollection[S] = jcollect match {
    case x: PCollection[S] => x.base
    case _ => jcollect
  }

  protected def baseCheck(collect: JCollection[S]) = collect match {
    case x: PCollection[S] => x.base
    case _ => collect
  }

  def ++ (other: JCollection[S]) = union(other)

  override def parallelDo[T](fn: DoFn[S, T], ptype: PType[T]) = {
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

  override def write(target: Target) = jcollect.write(target)

  override def getPType() = jcollect.getPType()

  override def getTypeFamily() = jcollect.getTypeFamily()

  override def getSize() = jcollect.getSize()

  override def getName() = jcollect.getName()
}

trait SDoFn[S, T] extends DoFn[S, T] with Function1[S, Traversable[T]] {
  override def process(input: S, emitter: Emitter[T]): Unit = {
    for (v <- apply(c2s(input).asInstanceOf[S])) {
      emitter.emit(s2c(v).asInstanceOf[T])
    }
  }
}

trait SDoFn2[S, K, V] extends DoFn[S, JPair[K, V]] with Function1[S, Traversable[(K, V)]] {
  override def process(input: S, emitter: Emitter[JPair[K, V]]): Unit = {
    for (v <- apply(c2s(input).asInstanceOf[S])) {
      emitter.emit(s2c(v).asInstanceOf[JPair[K, V]])
    }
  }
}

trait SFilterFn[T] extends FilterFn[T] with Function1[T, Boolean] {
  override def accept(input: T): Boolean = {
    apply(c2s(input).asInstanceOf[T]);
  }
}

trait SMapFn[S, T] extends MapFn[S, T] with Function1[S, T] {
  override def map(input: S): T = {
    s2c(apply(c2s(input).asInstanceOf[S])).asInstanceOf[T]
  }
}

trait SMapFn2[S, K, V] extends MapFn[S, JPair[K, V]] with Function1[S, (K, V)] {
  override def map(input: S): JPair[K, V] = {
    s2c(apply(c2s(input).asInstanceOf[S])).asInstanceOf[JPair[K, V]]
  }
}

trait SMapKeyFn[S, K] extends MapFn[S, JPair[K, S]] with Function1[S, K] {
  override def map(input: S): JPair[K, S] = {
    val sc = c2s(input).asInstanceOf[S]
    JPair.of(s2c(apply(sc)).asInstanceOf[K], input)
  }
}

class DSDoFn[S, T](fn: S => Traversable[T]) extends SDoFn[S, T] {
    ClosureCleaner.clean(fn)
  override def apply(x: S) = fn(x)
}

class DSDoFn2[S, K, V](fn: S => Traversable[(K, V)]) extends SDoFn2[S, K, V] {
  ClosureCleaner.clean(fn)
  override def apply(x: S) = fn(x)
}

class DSFilterFn[S](fn: S => Boolean) extends SFilterFn[S] {
  ClosureCleaner.clean(fn)
  override def apply(x: S) = fn(x)
}

class DSMapFn[S, T](fn: S => T) extends SMapFn[S, T] {
  ClosureCleaner.clean(fn)
  override def apply(x: S) = fn(x)
}

class DSMapFn2[S, K, V](fn: S => (K, V)) extends SMapFn2[S, K, V] {
  ClosureCleaner.clean(fn)
  override def apply(x: S) = fn(x)
}

class DSMapKeyFn[S, K](fn: S => K) extends SMapKeyFn[S, K] {
  ClosureCleaner.clean(fn)
  override def apply(x: S) = fn(x)
}
