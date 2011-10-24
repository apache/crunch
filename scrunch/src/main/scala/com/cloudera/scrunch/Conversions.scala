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

import com.cloudera.crunch.{PCollection => JCollection, PGroupedTable => JGroupedTable, PTable => JTable, DoFn}
import com.cloudera.crunch.{Pair => CPair, Tuple3 => CTuple3, Tuple4 => CTuple4, Tuple => CTuple, TupleN => CTupleN}
import com.cloudera.crunch.`type`.{PType, PTypeFamily};
import java.lang.{Boolean => JBoolean, Double => JDouble, Float => JFloat, Integer => JInteger, Long => JLong}
import java.lang.{Iterable => JIterable}
import java.nio.ByteBuffer
import scala.collection.{Iterable, Iterator}

trait PTypeH[T] {
  def getPType(ptf: PTypeFamily): PType[T]
}

trait CanParallelTransform[El, To] {
  def apply[A](c: PCollectionLike[A, _, JCollection[A]], fn: DoFn[_, _], ptype: PType[El]): To
}

trait LowPriorityParallelTransforms {
  implicit def single[B] = new CanParallelTransform[B, PCollection[B]] {
    def apply[A](c: PCollectionLike[A, _, JCollection[A]], fn: DoFn[_, _], ptype: PType[B]) = {
      c.parallelDo(fn.asInstanceOf[DoFn[A, B]], ptype)
    }
  }
}

object CanParallelTransform extends LowPriorityParallelTransforms {
  def tableType[K, V](ptype: PType[(K, V)]) = {
    val st = ptype.getSubTypes()
    ptype.getFamily().tableOf(st.get(0).asInstanceOf[PType[K]], st.get(1).asInstanceOf[PType[V]])
  }

  implicit def keyvalue[K, V] = new CanParallelTransform[(K, V), PTable[K,V]] {
    def apply[A](c: PCollectionLike[A, _, JCollection[A]], fn: DoFn[_, _], ptype: PType[(K, V)]) = {
      c.parallelDo(fn.asInstanceOf[DoFn[A, CPair[K, V]]], tableType(ptype))
    }
  }
} 

class ConversionIterator[S](iterator: java.util.Iterator[S]) extends Iterator[S] {
  override def hasNext() = iterator.hasNext()
  override def next() = Conversions.c2s(iterator.next()).asInstanceOf[S]
}

class ConversionIterable[S](iterable: JIterable[S]) extends Iterable[S] {
  override def iterator() = new ConversionIterator[S](iterable.iterator())
}

object Conversions {

  implicit val longs = new PTypeH[Long] { def getPType(ptf: PTypeFamily) = ptf.longs().asInstanceOf[PType[Long]] }
  implicit val ints = new PTypeH[Int] { def getPType(ptf: PTypeFamily) = ptf.ints().asInstanceOf[PType[Int]] }
  implicit val floats = new PTypeH[Float] { def getPType(ptf: PTypeFamily) = ptf.floats().asInstanceOf[PType[Float]] }
  implicit val doubles = new PTypeH[Double] { def getPType(ptf: PTypeFamily) = ptf.doubles().asInstanceOf[PType[Double]] }
  implicit val strings = new PTypeH[String] { def getPType(ptf: PTypeFamily) = ptf.strings() }
  implicit val booleans = new PTypeH[Boolean] { def getPType(ptf: PTypeFamily) = ptf.booleans().asInstanceOf[PType[Boolean]] }
  implicit val bytes = new PTypeH[ByteBuffer] { def getPType(ptf: PTypeFamily) = ptf.bytes() }

  implicit def collections[T: PTypeH] = {
    new PTypeH[Iterable[T]] {
      def getPType(ptf: PTypeFamily) = {
        ptf.collections(implicitly[PTypeH[T]].getPType(ptf)).asInstanceOf[PType[Iterable[T]]]
      }
    }
  }

  implicit def pairs[A: PTypeH, B: PTypeH] = {
    new PTypeH[(A, B)] {
      def getPType(ptf: PTypeFamily) = {
        ptf.pairs(implicitly[PTypeH[A]].getPType(ptf), implicitly[PTypeH[B]].getPType(ptf)).asInstanceOf[PType[(A, B)]]
      }
    }
  }

  implicit def trips[A: PTypeH, B: PTypeH, C: PTypeH] = {
    new PTypeH[(A, B, C)] {
      def getPType(ptf: PTypeFamily) = {
        ptf.triples(implicitly[PTypeH[A]].getPType(ptf), implicitly[PTypeH[B]].getPType(ptf),
            implicitly[PTypeH[C]].getPType(ptf)).asInstanceOf[PType[(A, B, C)]]
      }
    }
  }

  implicit def quads[A: PTypeH, B: PTypeH, C: PTypeH, D: PTypeH] = {
    new PTypeH[(A, B, C, D)] {
      def getPType(ptf: PTypeFamily) = {
        ptf.quads(implicitly[PTypeH[A]].getPType(ptf), implicitly[PTypeH[B]].getPType(ptf),
            implicitly[PTypeH[C]].getPType(ptf), implicitly[PTypeH[D]].getPType(ptf)).asInstanceOf[PType[(A, B, C, D)]]
      }
    }
  }

  implicit def jtable2ptable[K, V](jtable: JTable[K, V]) = {
    new PTable[K, V](jtable)
  }
  
  implicit def jcollect2pcollect[S](jcollect: JCollection[S]) = {
    new PCollection[S](jcollect)
  }
  
  implicit def jgrouped2pgrouped[K, V](jgrouped: JGroupedTable[K, V]) = {
    new PGroupedTable[K, V](jgrouped)
  }

  implicit def pair2tuple[K, V](p: CPair[K, V]) = (p.first(), p.second())

  implicit def tuple2pair[K, V](t: (K, V)) = CPair.of(t._1, t._2)

  def s2c(obj: Any): Any = obj match {
    case x: Tuple2[_, _] =>  CPair.of(s2c(x._1), s2c(x._2))
    case x: Tuple3[_, _, _] => new CTuple3(s2c(x._1), s2c(x._2), s2c(x._3))
    case x: Tuple4[_, _, _, _] => new CTuple4(s2c(x._1), s2c(x._2), s2c(x._3), s2c(x._4))
    case x: Product => new CTupleN(x.productIterator.map(s2c).toList.toArray)
    case _ => obj
  }

  def c2s(obj: Any): Any = obj match {
    case x: CTuple => {
      val v = (0 until x.size).map((i: Int) => c2s(x.get(i))).toArray
      v.length match {
       case 2 => Tuple2(v(0), v(1))
       case 3 => Tuple3(v(0), v(1), v(2))
       case 4 => Tuple4(v(0), v(1), v(2), v(3))
       case 5 => Tuple5(v(0), v(1), v(2), v(3), v(4))
       case 6 => Tuple6(v(0), v(1), v(2), v(3), v(4), v(5))
       case 7 => Tuple7(v(0), v(1), v(2), v(3), v(4), v(5), v(6))
       case 8 => Tuple8(v(0), v(1), v(2), v(3), v(4), v(5), v(6), v(7))
       case 9 => Tuple9(v(0), v(1), v(2), v(3), v(4), v(5), v(6), v(7), v(8))
       case 10 => Tuple10(v(0), v(1), v(2), v(3), v(4), v(5), v(6), v(7), v(8), v(9))
       case 11 => Tuple11(v(0), v(1), v(2), v(3), v(4), v(5), v(6), v(7), v(8), v(9), v(10))
       case 12 => Tuple12(v(0), v(1), v(2), v(3), v(4), v(5), v(6), v(7), v(8), v(9), v(10), v(11))
       case _ => { println("Seriously? A " + v.length + " tuple?"); obj }
     }
    }
    case x: java.util.Collection[_] => new ConversionIterable(x)
    case x: JLong => x.longValue()
    case x: JInteger => x.intValue()
    case x: JFloat => x.floatValue()
    case x: JDouble => x.doubleValue()
    case _ => obj
  }
}
