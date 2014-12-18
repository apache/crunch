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

import org.apache.crunch.{PCollection => JCollection, PGroupedTable => JGroupedTable, PTable => JTable, DoFn, Emitter}
import org.apache.crunch.{Pair => CPair}
import org.apache.crunch.types.{PTypes, PType}
import java.nio.ByteBuffer
import scala.collection.Iterable
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.TaskInputOutputContext
import com.google.protobuf.Message
import org.apache.avro.specific.SpecificRecord
import org.apache.thrift.{TFieldIdEnum, TBase}

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

      override def setContext(ctxt: TaskInputOutputContext[_, _, _, _]) {
        super.setContext(ctxt)
        fn.setContext(ctxt)
      }

      override def initialize() {
        fn.initialize()
      }

      override def process(input: A, emitFn: Emitter[CPair[K, V]]) {
        fn.process(input, new Emitter[(K, V)] {
          override def emit(kv: (K, V)) { emitFn.emit(CPair.of(kv._1, kv._2)) }
          override def flush() { emitFn.flush() }
        })
      }

      override def cleanup(emitFn: Emitter[CPair[K, V]]) {
        fn.cleanup(new Emitter[(K, V)] {
          override def emit(kv: (K, V)) { emitFn.emit(CPair.of(kv._1, kv._2)) }
          override def flush() { emitFn.flush() }
        })
      }
    }
  }
}

trait PTypeH[T] extends Serializable {
  def get(ptf: PTypeFamily): PType[T]
}

trait VeryLowPriorityPTypeH {
  implicit def records[T <: AnyRef : ClassTag] = new PTypeH[T] {
    def get(ptf: PTypeFamily) = ptf.records(implicitly[ClassTag[T]]).asInstanceOf[PType[T]]
  }
}

trait LowPriorityPTypeH extends VeryLowPriorityPTypeH {
  implicit def caseClasses[T <: Product: TypeTag] = new PTypeH[T] {
    override def get(ptf: PTypeFamily): PType[T] = ptf.caseClasses[T]
  }
}

object PTypeH extends GeneratedTupleConversions with LowPriorityPTypeH {

  implicit val longs = new PTypeH[Long] { def get(ptf: PTypeFamily) = ptf.longs }
  implicit val jlongs = new PTypeH[java.lang.Long] { def get(ptf: PTypeFamily) = ptf.jlongs }
  implicit val ints = new PTypeH[Int] { def get(ptf: PTypeFamily) = ptf.ints }
  implicit val jints = new PTypeH[java.lang.Integer] { def get(ptf: PTypeFamily) = ptf.jints }

  implicit val floats = new PTypeH[Float] { def get(ptf: PTypeFamily) = ptf.floats }
  implicit val jfloats = new PTypeH[java.lang.Float] { def get(ptf: PTypeFamily) = ptf.jfloats }

  implicit val doubles = new PTypeH[Double] { def get(ptf: PTypeFamily) = ptf.doubles }
  implicit val jdoubles = new PTypeH[java.lang.Double] { def get(ptf: PTypeFamily) = ptf.jdoubles }

  implicit val booleans = new PTypeH[Boolean] { def get(ptf: PTypeFamily) = ptf.booleans }
  implicit val jbooleans = new PTypeH[java.lang.Boolean] { def get(ptf: PTypeFamily) = ptf.jbooleans }

  implicit val strings = new PTypeH[String] { def get(ptf: PTypeFamily) = ptf.strings }
  implicit val bytes = new PTypeH[ByteBuffer] { def get(ptf: PTypeFamily) = ptf.bytes }

  implicit def jenums[E <: java.lang.Enum[E] : ClassTag] = new PTypeH[E] {
    def get(ptf: PTypeFamily): PType[E] = ptf.jenums(implicitly[ClassTag[E]])
  }

  implicit def writables[W <: Writable : ClassTag] = new PTypeH[W] {
    def get(ptf: PTypeFamily): PType[W] = ptf.writables(implicitly[ClassTag[W]])
  }

  implicit def protos[T <: Message : ClassTag] = new PTypeH[T] {
    def get(ptf: PTypeFamily) = {
      PTypes.protos(implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]], ptf.ptf)
    }
  }

  implicit def thrifts[T <: TBase[_ <: TBase[_, _], _ <: TFieldIdEnum] : ClassTag] = new PTypeH[T] {
    def get(ptf: PTypeFamily) = {
      PTypes.thrifts(implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]], ptf.ptf)
    }
  }

  implicit def specifics[T <: SpecificRecord : ClassTag] = new PTypeH[T] {
    def get(ptf: PTypeFamily) = Avros.specifics[T]()
  }

  implicit def options[T: PTypeH] = new PTypeH[Option[T]] {
    def get(ptf: PTypeFamily) = {
      ptf.options(implicitly[PTypeH[T]].get(ptf))
    }
  }

  implicit def eithers[L: PTypeH, R: PTypeH] = new PTypeH[Either[L, R]] {
    def get(ptf: PTypeFamily) = {
      ptf.eithers(implicitly[PTypeH[L]].get(ptf), implicitly[PTypeH[R]].get(ptf))
    }
  }

  implicit def arrays[T: PTypeH] = {
    new PTypeH[Array[T]] {
      def get(ptf: PTypeFamily) = {
        ptf.arrays[T](implicitly[PTypeH[T]].get(ptf))
      }
    }
  }

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

  implicit def maps[K: PTypeH, V: PTypeH] = {
    new PTypeH[Map[K, V]] {
      def get(ptf: PTypeFamily) = {
        ptf.maps(implicitly[PTypeH[K]].get(ptf), implicitly[PTypeH[V]].get(ptf))
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
  
  implicit def ptable2pcollect[K, V](table: PTable[K, V]): PCollection[(K, V)] = table.asPCollection()
}
