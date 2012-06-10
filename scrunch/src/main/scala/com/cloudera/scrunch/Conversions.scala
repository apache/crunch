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

import com.cloudera.crunch.{PCollection => JCollection, PGroupedTable => JGroupedTable, PTable => JTable, DoFn, Emitter}
import com.cloudera.crunch.{Pair => CPair}
import com.cloudera.crunch.types.PType
import java.nio.ByteBuffer
import scala.collection.Iterable

trait CanParallelTransform[El, To] {
  def apply[A](c: PCollectionLike[A, _, JCollection[A]], fn: DoFn[A, El], ptype: PType[El]): To
}

trait LowPriorityParallelTransforms {
  implicit def single[B] = new CanParallelTransform[B, PCollection[B]] {
    def apply[A](c: PCollectionLike[A, _, JCollection[A]], fn: DoFn[A, B], ptype: PType[B]) = {
      c.parallelDo(fn, ptype)
    }
  }
}

object CanParallelTransform extends LowPriorityParallelTransforms {
  def tableType[K, V](ptype: PType[(K, V)]) = {
    val st = ptype.getSubTypes()
    ptype.getFamily().tableOf(st.get(0).asInstanceOf[PType[K]], st.get(1).asInstanceOf[PType[V]])
  }

  implicit def keyvalue[K, V] = new CanParallelTransform[(K, V), PTable[K,V]] {
    def apply[A](c: PCollectionLike[A, _, JCollection[A]], fn: DoFn[A, (K, V)], ptype: PType[(K, V)]) = {
      c.parallelDo(kvWrapFn(fn), tableType(ptype))
    }
  }

  def kvWrapFn[A, K, V](fn: DoFn[A, (K, V)]) = {
    new DoFn[A, CPair[K, V]] {
      override def process(input: A, emitFn: Emitter[CPair[K, V]]) {
        fn.process(input, new Emitter[(K, V)] {
          override def emit(kv: (K, V)) { emitFn.emit(CPair.of(kv._1, kv._2)) }
          override def flush() { emitFn.flush() }
        })
      }
    }
  }
} 

trait PTypeH[T] {
  def get(ptf: PTypeFamily): PType[T]
}

object PTypeH {

  implicit val longs = new PTypeH[Long] { def get(ptf: PTypeFamily) = ptf.longs }
  implicit val ints = new PTypeH[Int] { def get(ptf: PTypeFamily) = ptf.ints }
  implicit val floats = new PTypeH[Float] { def get(ptf: PTypeFamily) = ptf.floats }
  implicit val doubles = new PTypeH[Double] { def get(ptf: PTypeFamily) = ptf.doubles }
  implicit val strings = new PTypeH[String] { def get(ptf: PTypeFamily) = ptf.strings }
  implicit val booleans = new PTypeH[Boolean] { def get(ptf: PTypeFamily) = ptf.booleans }
  implicit val bytes = new PTypeH[ByteBuffer] { def get(ptf: PTypeFamily) = ptf.bytes }

  implicit def collections[T: PTypeH] = {
    new PTypeH[Iterable[T]] {
      def get(ptf: PTypeFamily) = {
        ptf.collections(implicitly[PTypeH[T]].get(ptf))
      }
    }
  }

  implicit def lists[T: PTypeH] = {
    new PTypeH[List[T]] {
      def get(ptf: PTypeFamily) = {
        ptf.lists(implicitly[PTypeH[T]].get(ptf))
      }
    }
  }

  implicit def sets[T: PTypeH] = {
    new PTypeH[Set[T]] {
      def get(ptf: PTypeFamily) = {
        ptf.sets(implicitly[PTypeH[T]].get(ptf))
      }
    }
  }

  implicit def pairs[A: PTypeH, B: PTypeH] = {
    new PTypeH[(A, B)] {
      def get(ptf: PTypeFamily) = {
        ptf.tuple2(implicitly[PTypeH[A]].get(ptf), implicitly[PTypeH[B]].get(ptf))
      }
    }
  }

  implicit def trips[A: PTypeH, B: PTypeH, C: PTypeH] = {
    new PTypeH[(A, B, C)] {
      def get(ptf: PTypeFamily) = {
        ptf.tuple3(implicitly[PTypeH[A]].get(ptf), implicitly[PTypeH[B]].get(ptf),
            implicitly[PTypeH[C]].get(ptf))
      }
    }
  }

  implicit def quads[A: PTypeH, B: PTypeH, C: PTypeH, D: PTypeH] = {
    new PTypeH[(A, B, C, D)] {
      def get(ptf: PTypeFamily) = {
        ptf.tuple4(implicitly[PTypeH[A]].get(ptf), implicitly[PTypeH[B]].get(ptf),
            implicitly[PTypeH[C]].get(ptf), implicitly[PTypeH[D]].get(ptf))
      }
    }
  }

  implicit def records[T <: AnyRef : ClassManifest] = new PTypeH[T] {
    def get(ptf: PTypeFamily) = ptf.records(classManifest[T]).asInstanceOf[PType[T]]
  }
}

object Conversions {
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
}
