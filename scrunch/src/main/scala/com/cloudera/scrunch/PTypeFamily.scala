/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.scrunch

import com.cloudera.crunch.{Pair => CPair, Tuple3 => CTuple3, Tuple4 => CTuple4, MapFn}
import com.cloudera.crunch.types.{PType, PTypeFamily => PTF}
import com.cloudera.crunch.types.writable.WritableTypeFamily
import com.cloudera.crunch.types.avro.{AvroTypeFamily, Avros => CAvros}
import java.lang.{Long => JLong, Double => JDouble, Integer => JInt, Float => JFloat, Boolean => JBoolean}
import java.util.{Collection => JCollection}
import scala.collection.JavaConversions._

class TMapFn[S, T](f: S => T) extends MapFn[S, T] {
  override def map(input: S) = f(input)
}

trait PTypeFamily {

  def ptf: PTF

  val strings = ptf.strings()

  val bytes = ptf.bytes()

  def records[T: ClassManifest] = ptf.records(classManifest[T].erasure)

  def derived[S, T](cls: java.lang.Class[T], in: S => T, out: T => S, pt: PType[S]) = {
    ptf.derived(cls, new TMapFn[S, T](in), new TMapFn[T, S](out), pt)
  }

  val longs = {
    val in = (x: JLong) => x.longValue()
    val out = (x: Long) => new JLong(x)
    derived(classOf[Long], in, out, ptf.longs())
  }

  val ints = {
    val in = (x: JInt) => x.intValue()
    val out = (x: Int) => new JInt(x)
    derived(classOf[Int], in, out, ptf.ints())
  }

  val floats = {
    val in = (x: JFloat) => x.floatValue()
    val out = (x: Float) => new JFloat(x)
    derived(classOf[Float], in, out, ptf.floats())
  }

  val doubles = {
    val in = (x: JDouble) => x.doubleValue()
    val out = (x: Double) => new JDouble(x)
    derived(classOf[Double], in, out, ptf.doubles())
  }

  val booleans = {
    val in = (x: JBoolean) => x.booleanValue()
    val out = (x: Boolean) => new JBoolean(x)
    derived(classOf[Boolean], in, out, ptf.booleans())
  }

  def collections[T](ptype: PType[T]) = {
    derived(classOf[Iterable[T]], collectionAsScalaIterable[T], asJavaCollection[T], ptf.collections(ptype))
  }

  def maps[T](ptype: PType[T]) = {
    derived(classOf[scala.collection.Map[String, T]], mapAsScalaMap[String, T], mapAsJavaMap[String, T], ptf.maps(ptype))
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

  def tableOf[K, V](keyType: PType[K], valueType: PType[V]) = ptf.tableOf(keyType, valueType)
}

object Writables extends PTypeFamily {
  override def ptf = WritableTypeFamily.getInstance()
}

object Avros extends PTypeFamily {
  override def ptf = AvroTypeFamily.getInstance()

  CAvros.REFLECT_DATA_FACTORY = new ScalaReflectDataFactory()

  def reflects[T: ClassManifest]() = CAvros.reflects(classManifest[T].erasure)
}
