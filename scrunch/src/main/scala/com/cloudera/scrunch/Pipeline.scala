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

import com.cloudera.crunch.{PCollection => JCollection, Pipeline => JPipeline}
import com.cloudera.crunch.{Source, TableSource, Target}
import com.cloudera.crunch.impl.mr.MRPipeline
import org.apache.hadoop.conf.Configuration
import Conversions._

class Pipeline[R: ClassManifest](conf: Configuration = new Configuration()) {
  val jpipeline = new MRPipeline(classManifest[R].erasure, conf)

  def getConfiguration() = jpipeline.getConfiguration()

  def read[T](source: Source[T]) = new PCollection[T](jpipeline.read(source))

  def read[K, V](source: TableSource[K, V]) = {
    new PTable[K, V](jpipeline.read(source))
  }

  def write(pcollect: PCollection[_], target: Target) {
    jpipeline.write(pcollect.native, target)
  }

  def run() { jpipeline.run() }

  def done() { jpipeline.done() }

  def readTextFile(pathName: String) = new PCollection[String](jpipeline.readTextFile(pathName))
  
  def writeTextFile[T](pcollect: PCollection[T], pathName: String) {
    jpipeline.writeTextFile(pcollect.native, pathName)
  }
}
