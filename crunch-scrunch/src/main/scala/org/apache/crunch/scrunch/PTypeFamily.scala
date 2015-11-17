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

import org.apache.crunch.{Pair => CPair, Tuple3 => CTuple3, Tuple4 => CTuple4, TupleN, Union, MapFn}
import org.apache.crunch.types.{PType, PTypeFamily => PTF, PTypes}
import org.apache.crunch.types.writable.{WritableTypeFamily, Writables => CWritables}
import org.apache.crunch.types.avro.{AvroType, AvroTypeFamily, Avros => CAvros}
import java.lang.{Long => JLong, Double => JDouble, Integer => JInt, Float => JFloat, Boolean => JBoolean}
import java.lang.reflect.{Array => RArray}
import java.util.{Collection => JCollection}
import scala.collection.JavaConversions._
import scala.collection.mutable.{ListBuffer, Set => MSet, Map => MMap}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.reflect.runtime.currentMirror
import org.apache.hadoop.io.Writable
import org.apache.avro.specific.SpecificRecord
import java.nio.ByteBuffer
import com.google.common.collect.Lists

class TMapFn[S, T](val f: S => T, val pt: Option[PType[S]] = None, var init: Boolean = false) extends MapFn[S, T] {
  override def initialize() {
    if (!pt.isEmpty && getConfiguration() != null) {
      pt.get.initialize(getConfiguration())
      init = true
    }
  }

  override def map(input: S): T = {
    if (input == null) {
      return null.asInstanceOf[T]
    } else if (init) {
      return f(pt.get.getDetachedValue(input))
    } else {
      return f(input)
    }
  }
}

object GeneratedTupleHelper {
  def tupleN(args: Any*) = {
    TupleN.of(args.map(_.asInstanceOf[AnyRef]): _*)
  }
}

class TypeMapFn[P <: Product](val rc: Class[_], @transient var ctor: java.lang.reflect.Constructor[_] = null)
  extends MapFn[TupleN, P] {

  override def initialize {
    this.ctor = rc.getConstructors().apply(0)
  }

  override def map(x: TupleN): P = {
    if (x == null) {
      return null.asInstanceOf[P]
    }
    ctor.newInstance(x.getValues : _*).asInstanceOf[P]
  }
}

trait BasePTypeFamily {
  def ptf: PTF

  def derived[S, T](cls: java.lang.Class[T], in: S => T, out: T => S, pt: PType[S]) = {
    ptf.derived(cls, new TMapFn[S, T](in, Some(pt)), new TMapFn[T, S](out), pt)
  }
}

trait PTypeFamily extends GeneratedTuplePTypeFamily {

  def writables[T <: Writable](clazz: Class[T]): PType[T]

  def writables[T <: Writable : ClassTag]: PType[T] = {
    writables[T](implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]])
  }

  def as[T](ptype: PType[T]) = ptf.as(ptype)

  val strings = ptf.strings()

  val bytes = ptf.bytes()

  def records[T: ClassTag]: PType[T] = records(implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]])

  def records[T](clazz: Class[T]): PType[T] = ptf.records(clazz)

  def derivedImmutable[S, T](cls: java.lang.Class[T], in: S => T, out: T => S, pt: PType[S]) = {
    ptf.derivedImmutable(cls, new TMapFn[S, T](in), new TMapFn[T, S](out), pt)
  }

  val jlongs = ptf.longs()

  val longs = {
    val in = (x: JLong) => x.longValue()
    val out = (x: Long) => new JLong(x)
    derivedImmutable(classOf[Long], in, out, ptf.longs())
  }

  val jints = ptf.ints()

  val ints = {
    val in = (x: JInt) => x.intValue()
    val out = (x: Int) => new JInt(x)
    derivedImmutable(classOf[Int], in, out, ptf.ints())
  }

  val jfloats = ptf.floats()

  val floats = {
    val in = (x: JFloat) => x.floatValue()
    val out = (x: Float) => new JFloat(x)
    derivedImmutable(classOf[Float], in, out, ptf.floats())
  }

  val jdoubles = ptf.doubles()

  val doubles = {
    val in = (x: JDouble) => x.doubleValue()
    val out = (x: Double) => new JDouble(x)
    derivedImmutable(classOf[Double], in, out, ptf.doubles())
  }

  val jbooleans = ptf.booleans()

  val booleans = {
    val in = (x: JBoolean) => x.booleanValue()
    val out = (x: Boolean) => new JBoolean(x)
    derivedImmutable(classOf[Boolean], in, out, ptf.booleans())
  }

  def jenums[E <: java.lang.Enum[E] : ClassTag]: PType[E] = {
    PTypes.enums(implicitly[ClassTag[E]].runtimeClass.asInstanceOf[Class[E]], ptf).asInstanceOf[PType[E]]
  }

  def options[T](ptype: PType[T]) = {
    val in: Union => Option[T] = (x: Union) => { if (x.getIndex() == 0) None else Some(x.getValue.asInstanceOf[T]) }
    val out = (x: Option[T]) => { if (x.isEmpty) new Union(0, null) else new Union(1, x.get) }
    derived(classOf[Option[T]], in, out, ptf.unionOf(ptf.nulls(), ptype))
  }

  def eithers[L, R](left: PType[L], right: PType[R]): PType[Either[L, R]] = {
    val in: Union => Either[L, R] = (x: Union) => {
      if (x.getIndex() == 0) {
        Left[L, R](x.getValue.asInstanceOf[L])
      } else {
        Right[L, R](x.getValue.asInstanceOf[R])
      }
    }
    val out = (x: Either[L, R]) => { if (x.isLeft) new Union(0, x.left.get) else new Union(1, x.right.get) }
    derived(classOf[Either[L, R]], in, out, ptf.unionOf(left, right))
  }

  def tableOf[K, V](keyType: PType[K], valueType: PType[V]) = ptf.tableOf(keyType, valueType)

  def collections[T](ptype: PType[T]) = {
    derived(classOf[Iterable[T]], collectionAsScalaIterable[T], asJavaCollection[T], ptf.collections(ptype))
  }

  def maps[T](ptype: PType[T]): PType[Map[String, T]] = maps(strings, ptype)

  def maps[K, V](keyType: PType[K], valueType: PType[V]): PType[Map[K, V]] = {
    if (classOf[String].equals(keyType.getTypeClass)) {
      derived(classOf[Map[String, V]],
        { x: java.util.Map[String, V] => mapAsScalaMap(x).toMap},
        mapAsJavaMap[String, V],
        ptf.maps(valueType)).asInstanceOf[PType[Map[K, V]]]
    } else {
      derived(classOf[Map[K, V]],
        {x: JCollection[CPair[K, V]] => Map[K, V](x.map(y => (y.first(), y.second())).toArray : _*)},
        {x: Map[K, V] => asJavaCollection(x.toIterable.map(y => CPair.of(y._1, y._2)))},
        ptf.collections(ptf.pairs(keyType, valueType)))
    }
  }

  def mutableMaps[K, V](keyType: PType[K], valueType: PType[V]): PType[MMap[K, V]] = {
    derived(classOf[MMap[K, V]],
        {x: JCollection[CPair[K, V]] => MMap[K, V](x.map(y => (y.first(), y.second())).toArray : _*)},
        {x: MMap[K, V] => asJavaCollection(x.toIterable.map(y => CPair.of(y._1, y._2)))},
        ptf.collections(ptf.pairs(keyType, valueType)))
  }

  def arrays[T](ptype: PType[T]): PType[Array[T]] = {
    val in = (x: JCollection[_]) => {
      val ret = RArray.newInstance(ptype.getTypeClass, x.size())
      var i = 0
      val iter = x.iterator()
      while (iter.hasNext) {
        RArray.set(ret, i, iter.next())
        i += 1
      }
      ret.asInstanceOf[Array[T]]
    }
    val out = (x: Array[T]) => Lists.newArrayList(x: _*).asInstanceOf[JCollection[_]]
    derived(classOf[Array[T]],
      in, out,
      ptf.collections(ptype).asInstanceOf[PType[JCollection[_]]])
      .asInstanceOf[PType[Array[T]]]
  }

  def lists[T](ptype: PType[T]) = {
    val in = (x: JCollection[T]) => collectionAsScalaIterable[T](x).toList
    val out = (x: List[T]) => asJavaCollection[T](x)
    derived(classOf[List[T]], in, out, ptf.collections(ptype))
  }

  def listbuffers[T](ptype: PType[T]) = {
    val in = (x: JCollection[T]) => collectionAsScalaIterable[T](x).to[ListBuffer]
    val out = (x: ListBuffer[T]) => asJavaCollection[T](x)
    derived(classOf[ListBuffer[T]], in, out, ptf.collections(ptype))
  }

  def sets[T](ptype: PType[T]) = {
    val in = (x: JCollection[T]) => collectionAsScalaIterable[T](x).toSet
    val out = (x: Set[T]) => asJavaCollection[T](x)
    derived(classOf[Set[T]], in, out, ptf.collections(ptype))
  }

  def mutableSets[T](ptype: PType[T]) = {
    val in = (x: JCollection[T]) => collectionAsScalaIterable[T](x).to[MSet]
    val out = (x: MSet[T]) => asJavaCollection[T](x)
    derived(classOf[MSet[T]], in, out, ptf.collections(ptype))
  }

  def tuple2[T1, T2](p1: PType[T1], p2: PType[T2]) = {
    val in = (x: CPair[T1, T2]) => (x.first(), x.second())
    val out = (x: (T1, T2)) => CPair.of(x._1, x._2)
    derived(classOf[(T1, T2)], in, out, ptf.pairs(p1, p2))
  }

  def tuple3[T1, T2, T3](p1: PType[T1], p2: PType[T2], p3: PType[T3]) = {
    val in = (x: CTuple3[T1, T2, T3]) => (x.first(), x.second(), x.third())
    val out = (x: (T1, T2, T3)) => CTuple3.of(x._1, x._2, x._3)
    derived(classOf[(T1, T2, T3)], in, out, ptf.triples(p1, p2, p3))
  }

  def tuple4[T1, T2, T3, T4](p1: PType[T1], p2: PType[T2], p3: PType[T3], p4: PType[T4]) = {
    val in = (x: CTuple4[T1, T2, T3, T4]) => (x.first(), x.second(), x.third(), x.fourth())
    val out = (x: (T1, T2, T3, T4)) => CTuple4.of(x._1, x._2, x._3, x._4)
    derived(classOf[(T1, T2, T3, T4)], in, out, ptf.quads(p1, p2, p3, p4))
  }

  def namedTuples(tupleName: String, fields: List[(String, PType[_])]): PType[TupleN]

  def caseClasses[T <: Product : TypeTag]: PType[T] = products[T](implicitly[TypeTag[T]])

  private def products[T <: Product](typeTag: TypeTag[T]): PType[T] = {
    products(typeTag.tpe, typeTag.mirror)
  }

  private def products[T <: Product](tpe: Type, mirror: Mirror): PType[T] = {
    val ctor = tpe.member(nme.CONSTRUCTOR).asMethod
    val args = ctor.paramss.head.map(x => (x.name.toString,
      typeToPType(x.typeSignature, mirror)))
    val out = (x: Product) => TupleN.of(x.productIterator.toArray.asInstanceOf[Array[Object]] : _*)
    val rtc = mirror.runtimeClass(tpe)
    val base = namedTuples(getName(rtc) + "_", args) // See CRUNCH-495
    ptf.derived(rtc.asInstanceOf[Class[T]], new TypeMapFn[T](rtc), new TMapFn[T, TupleN](out), base)
  }

  private def getName(rtc: RuntimeClass): String = {
    try {
      rtc.getCanonicalName
    } catch {
      case e: InternalError => rtc.getName.replaceAllLiterally("$", "") // see CRUNCH-561
    }
  }

  private val classToPrimitivePType = Map(
    classOf[Int] -> ints,
    classOf[java.lang.Integer] -> jints,
    classOf[Long] -> longs,
    classOf[java.lang.Long] -> jlongs,
    classOf[Boolean] -> booleans,
    classOf[java.lang.Boolean] -> jbooleans,
    classOf[Double] -> doubles,
    classOf[java.lang.Double] -> jdoubles,
    classOf[Float] -> floats,
    classOf[java.lang.Float] -> jfloats,
    classOf[String] -> strings,
    classOf[ByteBuffer] -> bytes
  )

  private val typeToPTypeCache: collection.mutable.Map[Type, PType[_]] = new collection.mutable.HashMap()

  private def encache[T](tpe: Type, pt: PType[_]) = {
    typeToPTypeCache.put(tpe, pt)
    pt.asInstanceOf[PType[T]]
  }

  private def typeToPType[T](tpe: Type, mirror: Mirror): PType[T] = {
    val cpt = typeToPTypeCache.get(tpe)
    if (cpt.isDefined) {
      return cpt.get.asInstanceOf[PType[T]]
    }

    val rtc = mirror.runtimeClass(tpe)
    val ret = classToPrimitivePType.get(rtc)
    if (ret != null) {
      return ret.asInstanceOf[PType[T]]
    } else if (classOf[Writable].isAssignableFrom(rtc)) {
      return writables(rtc.asInstanceOf[Class[Writable]]).asInstanceOf[PType[T]]
    } else if (tpe.typeSymbol.asClass.isCaseClass) {
      return encache(tpe, products(tpe, mirror))
    } else {
      val targs = if (tpe.isInstanceOf[TypeRefApi]) {
        tpe.asInstanceOf[TypeRefApi].args
      } else {
        List()
      }

      if (targs.isEmpty) {
        return encache(tpe, records(rtc))
      } else if (targs.size == 1) {
        if (rtc.isArray) {
          return encache(tpe, arrays(typeToPType(targs(0), mirror)))
        } else if (classOf[List[_]].isAssignableFrom(rtc)) {
          return encache(tpe, lists(typeToPType(targs(0), mirror)))
        } else if (classOf[Set[_]].isAssignableFrom(rtc)) {
          return encache(tpe, sets(typeToPType(targs(0), mirror)))
        } else if (classOf[Option[_]].isAssignableFrom(rtc)) {
          return encache(tpe, options(typeToPType(targs(0), mirror)))
        } else if (classOf[Iterable[_]].isAssignableFrom(rtc)) {
          return encache(tpe, collections(typeToPType(targs(0), mirror)))
        }
      } else if (targs.size == 2) {
        if (classOf[Either[_, _]].isAssignableFrom(rtc)) {
          return encache(tpe, eithers(typeToPType(targs(0), mirror), typeToPType(targs(1), mirror)))
        } else if (classOf[Map[_, _]].isAssignableFrom(rtc)) {
          return encache(tpe, maps(typeToPType(targs(0), mirror), typeToPType(targs(1), mirror)))
        }
      }
    }
    throw new IllegalArgumentException("Could not handle class type = " + tpe)
  }
}

object Writables extends PTypeFamily {
  override def ptf = WritableTypeFamily.getInstance()

  override def writables[T <: Writable](clazz: Class[T]) = CWritables.writables(clazz)

  override def namedTuples(tupleName: String, fields: List[(String, PType[_])]) = {
    ptf.tuples(fields.map(_._2).toArray :_*)
  }
}

object Avros extends PTypeFamily {
  override def ptf = AvroTypeFamily.getInstance()

  override def writables[T <: Writable](clazz: Class[T]) = CAvros.writables(clazz)

  override def records[T: ClassTag] = reflects()(implicitly[ClassTag[T]])

  def specifics[T <: SpecificRecord : ClassTag]() = {
    CAvros.specifics(implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]])
  }

  def reflects[T: ClassTag](): AvroType[T] = {
    val clazz = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    val schema = ScalaSafeReflectData.getInstance().getSchema(clazz)
    CAvros.reflects(clazz, schema)
  }

  override def namedTuples(tupleName: String, fields: List[(String, PType[_])]) = {
    CAvros.namedTuples(tupleName, fields.map(_._1).toArray, fields.map(_._2).toArray)
  }
}
