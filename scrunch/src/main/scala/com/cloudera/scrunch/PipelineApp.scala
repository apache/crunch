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

import com.cloudera.crunch.{Source, TableSource, Target}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.util.GenericOptionsParser
import scala.collection.mutable.ListBuffer

trait PipelineApp extends DelayedInit {
  implicit def _string2path(str: String) = new Path(str)

  private val pipeline = new Pipeline(new Configuration())(ClassManifest.fromClass(getClass()))

  val from = From
  val to = To
  val at = At

  def configuration = pipeline.getConfiguration
  def fs: FileSystem = FileSystem.get(configuration)

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

  def cogroup[K: PTypeH, V1: PTypeH, V2: PTypeH](t1: PTable[K, V1], t2: PTable[K, V2]) = {
    t1.cogroup(t2)
  }

  def join[K: PTypeH, V1: PTypeH, V2: PTypeH](t1: PTable[K, V1], t2: PTable[K, V2]) = {
    t1.join(t2)
  }

  def run { pipeline.run }

  def done { pipeline.done }

  private var _args: Array[String] = _

  protected def args: Array[String] = _args

  private val initCode = new ListBuffer[() => Unit]

  override def delayedInit(body: => Unit) {
    initCode += (() => body)
  }

  def main(args: Array[String]) = {
    val parser = new GenericOptionsParser(configuration, args)
    _args = parser.getRemainingArgs()
    for (proc <- initCode) proc()
    done
  }
}
