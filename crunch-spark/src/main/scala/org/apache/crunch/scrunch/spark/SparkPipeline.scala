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
package org.apache.crunch.scrunch.spark

import org.apache.crunch.scrunch.Pipeline
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf

import scala.reflect.ClassTag

/**
 * A Scrunch {@code Pipeline} instance that wraps an underlying
 * {@link org.apache.crunch.impl.spark.SparkPipeline} for executing
 * pipelines on Spark.
 */
class SparkPipeline(val master: String, val name: String, val clazz: Class[_],
                    val conf: Configuration) extends Pipeline({
  new org.apache.crunch.impl.spark.SparkPipeline(master, name, clazz, conf)
})

/**
 * Companion object for creating {@code SparkPipeline} instances.
 */
object SparkPipeline {

  /**
   * Default factory method for creating SparkPipeline instances.
   *
   * @tparam T The class to use for finding the right client JAR
   * @return A new SparkPipeline instance
   */
  def apply[T: ClassTag](): SparkPipeline = apply[T](new Configuration())

  /**
   * Factory method that gets the name of the app and the Spark master
   * from the {@code spark.app.name} and {@code spark.master} properties.
   *
   * @param conf The Configuration instance to use
   * @tparam T The class to use for finding the right client JAR
   * @return A new SparkPipeline instance
   */
  def apply[T: ClassTag](conf: Configuration): SparkPipeline = {
    val sconf = new SparkConf()
    val name = conf.get("spark.app.name", sconf.get("spark.app.name", "ScrunchApp"))
    apply[T](name, conf)
  }

  /**
   * Factory method for SparkPipeline that gets the Spark master from the
   * {@code spark.master} property.
   *
   * @param name The name of the pipeline instance
   * @param conf A Configuration instance
   * @tparam T The class to use for finding the right client JAR
   * @return A new SparkPipeline
   */
  def apply[T: ClassTag](name: String, conf: Configuration): SparkPipeline = {
    val sconf = new SparkConf()
    val master = conf.get("spark.master", sconf.get("spark.master", "local"))
    apply[T](master, name, conf)
  }

  /**
   * Factory method for SparkPipeline.
   *
   * @param master The URL or code for the Spark master to use.
   * @param name The name of the pipeline instance
   * @param conf A Configuration instance
   * @tparam T The class to use for finding the right client JAR
   * @return A new SparkPipeline
   */
  def apply[T: ClassTag](master: String, name: String, conf: Configuration): SparkPipeline = {
    conf.set("spark.closure.serializer", "org.apache.crunch.scrunch.spark.ScrunchSerializer")
    new SparkPipeline(master, name, implicitly[ClassTag[T]].runtimeClass, conf)
  }
}
