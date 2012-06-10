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

import java.io.Serializable

import scala.collection.mutable.ListBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.util.GenericOptionsParser

import com.cloudera.crunch.{Source, TableSource, Target}
import com.cloudera.scrunch.Pipeline.PReader

trait PipelineApp extends MREmbeddedPipeline with PipelineHelper with DelayedInit {
  implicit def _string2path(str: String) = new Path(str)

  /** Contains factory methods used to create `Source`s. */
  val from = From

  /** Contains factory methods used to create `Target`s. */
  val to = To

  /** Contains factory methods used to create `SourceTarget`s. */
  val at = At

  private val initCode = new ListBuffer[() => Unit]

  private var _args: Array[String] = _

  /** Command-line arguments passed to this application. */
  protected def args: Array[String] = _args

  def configuration: Configuration = pipeline.getConfiguration

  /** Gets the distributed filesystem associated with this application's configuration. */
  def fs: FileSystem = FileSystem.get(configuration)

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
