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

import com.cloudera.crunch.{Source, TableSource}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser
import scala.collection.mutable.ListBuffer

trait PipelineApp extends DelayedInit {
  private val pipeline = new Pipeline(new Configuration())(ClassManifest.fromClass(getClass()))
  protected val from = From
  protected val to = To
  protected val at = At

  protected def configuration = pipeline.getConfiguration

  protected def read[T](source: Source[T]) = pipeline.read(source)

  protected def read[K, V](tableSource: TableSource[K, V]) = pipeline.read(tableSource)

  protected def cogroup[K: PTypeH, V1: PTypeH, V2: PTypeH](t1: PTable[K, V1], t2: PTable[K, V2]) = {
    t1.cogroup(t2)
  }

  protected def join[K: PTypeH, V1: PTypeH, V2: PTypeH](t1: PTable[K, V1], t2: PTable[K, V2]) = {
    t1.join(t2)
  }

  protected def run { pipeline.run }

  protected def done { pipeline.done }

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
