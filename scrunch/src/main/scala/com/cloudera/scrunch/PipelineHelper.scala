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

/**
 * This trait provides convenience methods for building pipelines.
 */
trait PipelineHelper {
  /**
   * Materializes the specified PCollection and displays its contents.
   */
  def dump(data: PCollection[_]) {
    data.materialize.foreach(println(_))
  }

  /**
   * Materializes the specified PTable and displays its contents.
   */
  def dump(data: PTable[_, _]) {
    data.materialize.foreach(println(_))
  }

  /**
   * Performs a cogroup on the two specified PTables.
   */
  def cogroup[K : PTypeH, V1 : PTypeH, V2 : PTypeH](t1: PTable[K, V1], t2: PTable[K, V2])
      : PTable[K, (Iterable[V1], Iterable[V2])] = {
    t1.cogroup(t2)
  }

  /**
   * Performs an innerjoin on the two specified PTables.
   */
  def join[K : PTypeH, V1 : PTypeH, V2 : PTypeH](t1: PTable[K, V1], t2: PTable[K, V2])
      : PTable[K, (V1, V2)] = {
    t1.join(t2)
  }

  /**
   * Unions the specified PCollections.
   */
  def union[T](first: PCollection[T], others: PCollection[T]*)
      : PCollection[T] = {
    first.union(others: _*)
  }

  /**
   * Unions the specified PTables.
   */
  def union[K, V](first: PTable[K, V], others: PTable[K, V]*)
      : PTable[K, V] = {
    first.union(others: _*)
  }
}

/**
 * Companion object containing convenience methods for building pipelines.
 */
object PipelineHelper extends PipelineHelper
