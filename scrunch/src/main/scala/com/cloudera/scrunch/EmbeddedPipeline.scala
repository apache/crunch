/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
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

import org.apache.hadoop.conf.Configuration

import com.cloudera.crunch.Source
import com.cloudera.crunch.TableSource
import com.cloudera.crunch.Target
import com.cloudera.scrunch.Pipeline.PReader
import com.cloudera.scrunch.Pipeline.PWriter

trait EmbeddedPipeline {
  /** Used to create the configuration object for a pipeline. */
  protected def createConfiguration(): Configuration = new Configuration()

  /** The pipeline to use. */
  protected def pipeline: Pipeline[_]
}

trait MREmbeddedPipeline extends EmbeddedPipeline with EmbeddedPipelineLike {
  protected val pipeline: Pipeline[_] = {
    new Pipeline(createConfiguration())(ClassManifest.fromClass(getClass()))
  }
}

trait MemEmbeddedPipeline extends EmbeddedPipeline with EmbeddedPipelineLike {
  protected val pipeline: Pipeline[_] = {
    new Pipeline(createConfiguration(), true)(ClassManifest.fromClass(getClass()))
  }
}

