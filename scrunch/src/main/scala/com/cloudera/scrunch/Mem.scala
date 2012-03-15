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

import com.cloudera.crunch.{Pair => P}
import com.cloudera.crunch.{Source, TableSource, Target}
import com.cloudera.crunch.impl.mem.MemPipeline
import java.lang.{Iterable => JIterable}
import org.apache.hadoop.conf.Configuration
import scala.collection.JavaConversions._
import Conversions._

/**
 * Object for working with in-memory PCollection and PTable instances.
 */
object Mem {
  private val ptf = Avros

  val pipeline = new Pipeline(new Configuration(), true)(ClassManifest.fromClass(getClass()))

  def collectionOf[T](ts: T*)(implicit pt: PTypeH[T]): PCollection[T] = {
    collectionOf(List(ts:_*))
  }

  def collectionOf[T](collect: Iterable[T])(implicit pt: PTypeH[T]): PCollection[T] = {
    val native = MemPipeline.typedCollectionOf(pt.get(ptf), asJavaIterable(collect))
    new PCollection[T](native)
  }

  def tableOf[K, V](pairs: (K, V)*)(implicit pk: PTypeH[K], pv: PTypeH[V]): PTable[K, V] = {
    tableOf(List(pairs:_*))
  }

  def tableOf[K, V](pairs: Iterable[(K, V)])(implicit pk: PTypeH[K], pv: PTypeH[V]): PTable[K, V] = {
    val cpairs = pairs.map(kv => P.of(kv._1, kv._2))
    val ptype = ptf.tableOf(pk.get(ptf), pv.get(ptf))
    new PTable[K, V](MemPipeline.typedTableOf(ptype, asJavaIterable(cpairs)))
  }


  val from = From
  val to = To
  val at = At

  def read[T](source: Source[T]) = pipeline.read(source)

  def read[K, V](tableSource: TableSource[K, V]) = pipeline.read(tableSource)

  def load[T](source: Source[T]) = read(source)

  def load[K, V](tableSource: TableSource[K, V]) = read(tableSource)

  def write(data: PCollection[_], target: Target) {
    pipeline.write(data, target)
  }

  def write(data: PTable[_, _], target: Target) {
    pipeline.write(data, target)
  }

  def store(data: PCollection[_], target: Target) {
    pipeline.write(data, target)
  }

  def store(data: PTable[_, _], target: Target) {
    pipeline.write(data, target)
  }

  def dump(data: PCollection[_]) {
    data.materialize.foreach { println(_) }
  }

  def dump(data: PTable[_, _]) {
    data.materialize.foreach { println(_) }
  }
}
