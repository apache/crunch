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

import org.apache.crunch.{Pair => CPair, Tuple3 => CTuple3, Tuple4 => CTuple4, Union, MapFn}
import org.apache.crunch.types.{PType, PTypeFamily => PTF}
import org.apache.crunch.types.writable.{WritableTypeFamily, Writables => CWritables}
import org.apache.crunch.types.avro.{AvroType, AvroTypeFamily, Avros => CAvros}
import java.lang.{Long => JLong, Double => JDouble, Integer => JInt, Float => JFloat, Boolean => JBoolean}
import java.util.{Collection => JCollection}
import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import org.apache.hadoop.io.Writable
import org.apache.avro.specific.SpecificRecord

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

trait PTypeFamily {

  def ptf: PTF

  def writables[T <: Writable : ClassTag]: PType[T]

  def as[T](ptype: PType[T]) = ptf.as(ptype)

  val strings = ptf.strings()

  val bytes = ptf.bytes()

  def records[T: ClassTag] = ptf.records(implicitly[ClassTag[T]].runtimeClass)

  def derived[S, T](cls: java.lang.Class[T], in: S => T, out: T => S, pt: PType[S]) = {
    ptf.derived(cls, new TMapFn[S, T](in, Some(pt)), new TMapFn[T, S](out), pt)
  }

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

  def maps[T](ptype: PType[T]) = {
    derived(classOf[Map[String, T]], {x: java.util.Map[String, T] => mapAsScalaMap(x).toMap}, 
        mapAsJavaMap[String, T], ptf.maps(ptype))
  }

  def lists[T](ptype: PType[T]) = {
    val in = (x: JCollection[T]) => collectionAsScalaIterable[T](x).toList
    val out = (x: List[T]) => asJavaCollection[T](x)
    derived(classOf[List[T]], in, out, ptf.collections(ptype))
  }

  def sets[T](ptype: PType[T]) = {
    val in = (x: JCollection[T]) => collectionAsScalaIterable[T](x).toSet
    val out = (x: Set[T]) => asJavaCollection[T](x)
    derived(classOf[Set[T]], in, out, ptf.collections(ptype))
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
}

object Writables extends PTypeFamily {
  override def ptf = WritableTypeFamily.getInstance()

  override def writables[T <: Writable : ClassTag] = CWritables.writables(
    implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]])
}

object Avros extends PTypeFamily {
  override def ptf = AvroTypeFamily.getInstance()

  override def writables[T <: Writable : ClassTag] = CAvros.writables(
    implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]])

  override def records[T: ClassTag] = reflects()(implicitly[ClassTag[T]])

  def specifics[T <: SpecificRecord : ClassTag]() = {
    CAvros.specifics(implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]])
  }

  def reflects[T: ClassTag](): AvroType[T] = {
    val clazz = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    val schema = ScalaSafeReflectData.getInstance().getSchema(clazz)
    CAvros.reflects(clazz, schema)
  }
}
