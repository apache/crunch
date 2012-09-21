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

import java.io.Serializable

import scala.collection.mutable.ListBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.util.GenericOptionsParser

import org.apache.crunch.{Source, TableSource, Target}

trait PipelineApp extends MREmbeddedPipeline with PipelineHelper {
  implicit def _string2path(str: String) = new Path(str)

  /** Contains factory methods used to create `Source`s. */
  val from = From

  /** Contains factory methods used to create `Target`s. */
  val to = To

  /** Contains factory methods used to create `SourceTarget`s. */
  val at = At

  def configuration: Configuration = pipeline.getConfiguration

  /** Gets the distributed filesystem associated with this application's configuration. */
  def fs: FileSystem = FileSystem.get(configuration)

  /**
   * The entry-point for pipeline applications.  Clients should override this method to implement
   * the logic of their pipeline application.
   *
   * @param args The command-line arguments passed to the pipeline application.
   */
  def run(args: Array[String]): Unit

  final def main(args: Array[String]) = {
    val parser = new GenericOptionsParser(configuration, args)
    run(parser.getRemainingArgs())
    done
  }
}
