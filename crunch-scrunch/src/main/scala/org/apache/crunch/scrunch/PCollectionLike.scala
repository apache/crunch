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

import org.apache.crunch.{PCollection => JCollection, Pair => JPair, _}
import org.apache.crunch.types.{PType, PTableType}
import org.apache.crunch.types.writable.WritableTypeFamily
import org.apache.crunch.lib.Shard

/**
 * Base trait for PCollection-like entities in Scrunch, including PTables and PGroupedTables.
 *
 * @tparam S the data type of the underlying object contained in this instance
 * @tparam FullType the Scrunch PCollection type of this object
 * @tparam NativeType the corresponding Crunch PCollection type of this object
 */
trait PCollectionLike[S, +FullType, +NativeType <: JCollection[S]] {
  type FunctionType[T]
  type CtxtFunctionType[T]

  protected def wrapFlatMapFn[T](fmt: FunctionType[TraversableOnce[T]]): DoFn[S, T]
  protected def wrapMapFn[T](fmt: FunctionType[T]): MapFn[S, T]
  protected def wrapFilterFn(fmt: FunctionType[Boolean]): FilterFn[S]
  protected def wrapFlatMapWithCtxtFn[T](fmt: CtxtFunctionType[TraversableOnce[T]]): DoFn[S, T]
  protected def wrapMapWithCtxtFn[T](fmt: CtxtFunctionType[T]): MapFn[S, T]
  protected def wrapFilterWithCtxtFn(fmt: CtxtFunctionType[Boolean]): FilterFn[S]
  protected def wrapPairFlatMapFn[K, V](fmt: FunctionType[TraversableOnce[(K, V)]]): DoFn[S, JPair[K, V]]
  protected def wrapPairMapFn[K, V](fmt: FunctionType[(K, V)]): MapFn[S, JPair[K, V]]

  protected def setupRun() {
    PipelineLike.setupConf(native.getPipeline().getConfiguration())
  }

  /**
   * Returns the underlying PCollection wrapped by this instance.
   */
  val native: NativeType

  protected def wrap(newNative: JCollection[_]): FullType

  /**
   * Write the data in this instance to the given target.
   */
  def write(target: Target): FullType = wrap(native.write(target))

  /**
   * Write the data in this instance to the given target with the given {@link Target.WriteMode}.
   */
  def write(target: Target, writeMode: Target.WriteMode): FullType = {
    wrap(native.write(target, writeMode))
  }

  /**
   * Cache the data in this instance using the default caching mechanism for the
   * underlying Pipeline type.
   *
   * @return this instance
   */
  def cache() = wrap(native.cache())

  /**
   * Cache the data in this instance using the given caching options, if they are
   * applicable for the underlying Pipeline type.
   *
   * @return this instance
   */
  def cache(opts: CachingOptions) = wrap(native.cache(opts))

  /**
   * Apply a flatMap operation to this instance, returning a {@code PTable} if the return
   * type of the function is a {@code TraversableOnce[Tuple2]} and a {@code PCollection} otherwise.
   */
  def flatMap[T, To](f: FunctionType[TraversableOnce[T]])
                    (implicit pt: PTypeH[T], b: CanParallelTransform[T, To]): To = {
    b(this, wrapFlatMapFn(f), pt.get(getTypeFamily()))
  }

  /**
   * Apply a map operation to this instance, returning a {@code PTable} if the return
   * type of the function is a {@code Tuple2} and a {@code PCollection} otherwise.
   */
  def map[T, To](f: FunctionType[T])(implicit pt: PTypeH[T], b: CanParallelTransform[T, To]): To = {
    b(this, wrapMapFn(f), pt.get(getTypeFamily()))
  }

  /**
   * Apply the given filter function to the elements of this instance and return an new
   * instance that contains the items that pass the filter.
   */
  def filter(f: FunctionType[Boolean]): FullType = {
    wrap(native.filter(wrapFilterFn(f)))
  }

  def filter(name: String, f: FunctionType[Boolean]): FullType = {
    wrap(native.filter(name, wrapFilterFn(f)))
  }

  def flatMapWithContext[T, To](f: CtxtFunctionType[TraversableOnce[T]])
                               (implicit pt: PTypeH[T], b: CanParallelTransform[T, To]): To = {
    b(this, wrapFlatMapWithCtxtFn(f), pt.get(getTypeFamily()))
  }

  def mapWithContext[T, To](f: CtxtFunctionType[T])
                           (implicit pt: PTypeH[T], b: CanParallelTransform[T, To]): To = {
    b(this, wrapMapWithCtxtFn(f), pt.get(getTypeFamily()))
  }

  def filterWithContext(f: CtxtFunctionType[Boolean]): FullType = {
    wrap(native.filter(wrapFilterWithCtxtFn(f)))
  }

  /**
   * Applies the given doFn to the elements of this instance and
   * returns a new {@code PCollection} that is the output of this processing.
   */
  def parallelDo[T](fn: DoFn[S, T], ptype: PType[T]) = {
    new PCollection[T](native.parallelDo(fn, ptype))
  }
  /**
   * Applies the given doFn to the elements of this instance and
   * returns a new {@code PCollection} that is the output of this processing.
   */
  def parallelDo[T](name: String, fn: DoFn[S,T], ptype: PType[T]) = {
    new PCollection[T](native.parallelDo(name, fn, ptype))
  }

  /**
   * Applies the given doFn to the elements of this instance and
   * returns a new {@code PCollection} that is the output of this processing.
   */
  def parallelDo[T](name: String, fn: DoFn[S,T], ptype: PType[T], opts: ParallelDoOptions) = {
    new PCollection[T](native.parallelDo(name, fn, ptype, opts))
  }

  /**
   * Applies the given doFn to the elements of this instance and
   * returns a new {@code PTable} that is the output of this processing.
   */
  def parallelDo[K, V](fn: DoFn[S, JPair[K, V]], ptype: PTableType[K, V]) = {
    new PTable[K, V](native.parallelDo(fn, ptype))
  }

  /**
   * Applies the given doFn to the elements of this instance and
   * returns a new {@code PTable} that is the output of this processing.
   */
  def parallelDo[K, V](name: String, fn: DoFn[S, JPair[K, V]], ptype: PTableType[K, V]) = {
    new PTable[K, V](native.parallelDo(name, fn, ptype))
  }

  /**
   * Applies the given doFn to the elements of this instance and
   * returns a new {@code PTable} that is the output of this processing.
   */
  def parallelDo[K, V](name: String, fn: DoFn[S, JPair[K, V]], ptype: PTableType[K, V], opts: ParallelDoOptions) = {
    new PTable[K, V](native.parallelDo(name, fn, ptype, opts))
  }

  /**
   * Applies the given flatMap function to the elements of this instance and
   * returns a new {@code PCollection} that is the output of this processing.
   */
  def flatMap[T](f: FunctionType[TraversableOnce[T]], ptype: PType[T]) = {
    parallelDo(wrapFlatMapFn(f), ptype)
  }

  /**
   * Applies the given flatMap function to the elements of this instance and
   * returns a new {@code PCollection} that is the output of this processing.
   */
  def flatMap[T](name: String, f: FunctionType[TraversableOnce[T]], ptype: PType[T]) = {
    parallelDo(name, wrapFlatMapFn(f), ptype)
  }

  /**
   * Applies the given flatMap function to the elements of this instance and
   * returns a new {@code PCollection} that is the output of this processing.
   */
  def flatMap[T](name: String, f: FunctionType[TraversableOnce[T]], ptype: PType[T], opts: ParallelDoOptions) = {
    parallelDo(name, wrapFlatMapFn(f), ptype, opts)
  }

  /**
   * Applies the given flatMap function to the elements of this instance and
   * returns a new {@code PTable} that is the output of this processing.
   */
  def flatMap[K, V](f: FunctionType[TraversableOnce[(K, V)]], ptype: PTableType[K, V]) = {
    parallelDo(wrapPairFlatMapFn(f), ptype)
  }

  /**
   * Applies the given flatMap function to the elements of this instance and
   * returns a new {@code PTable} that is the output of this processing.
   */
  def flatMap[K, V](name: String, f: FunctionType[TraversableOnce[(K, V)]], ptype: PTableType[K, V]) = {
    parallelDo(name, wrapPairFlatMapFn(f), ptype)
  }

  /**
   * Applies the given flatMap function to the elements of this instance and
   * returns a new {@code PTable} that is the output of this processing.
   */
  def flatMap[K, V](name: String, f: FunctionType[TraversableOnce[(K, V)]], ptype: PTableType[K, V],
                    opts: ParallelDoOptions) = {
    parallelDo(name, wrapPairFlatMapFn(f), ptype, opts)
  }

  /**
   * Applies the given map function to the elements of this instance and
   * returns a new {@code PCollection} that is the output of this processing.
   */
  def map[T](f: FunctionType[T], ptype: PType[T]) = {
    parallelDo(wrapMapFn(f), ptype)
  }

  /**
   * Applies the given map function to the elements of this instance and
   * returns a new {@code PCollection} that is the output of this processing.
   */
  def map[T](name: String, f: FunctionType[T], ptype: PType[T]) = {
    parallelDo(name, wrapMapFn(f), ptype)
  }

  /**
   * Applies the given map function to the elements of this instance and
   * returns a new {@code PCollection} that is the output of this processing.
   */
  def map[T](name: String, f: FunctionType[T], ptype: PType[T], opts: ParallelDoOptions) = {
    parallelDo(name, wrapMapFn(f), ptype, opts)
  }

  /**
   * Applies the given map function to the elements of this instance and
   * returns a new {@code PTable} that is the output of this processing.
   */
  def map[K, V](f: FunctionType[(K, V)], ptype: PTableType[K, V]) = {
    parallelDo(wrapPairMapFn(f), ptype)
  }

  /**
   * Applies the given map function to the elements of this instance and
   * returns a new {@code PTable} that is the output of this processing.
   */
  def map[K, V](name: String, f: FunctionType[(K, V)], ptype: PTableType[K, V]) = {
    parallelDo(name, wrapPairMapFn(f), ptype)
  }

  /**
   * Applies the given map function to the elements of this instance and
   * returns a new {@code PTable} that is the output of this processing.
   */
  def map[K, V](name: String, f: FunctionType[(K, V)], ptype: PTableType[K, V],
                opts: ParallelDoOptions) = {
    parallelDo(name, wrapPairMapFn(f), ptype, opts)
  }

  /**
   * Re-partitions this instance into the given number of partitions.
   *
   * @param numPartitions the number of partitions to use
   * @return a re-partitioned version of the data in this instance
   */
  def shard(numPartitions: Int) = wrap(Shard.shard(native, numPartitions))

  /**
   * Adds this PCollection as a dependency for the given PipelineCallable
   * and then registers it to the Pipeline associated with this instance.
   */
  def sequentialDo[Output](label: String, fn: PipelineCallable[Output]) = {
    native.sequentialDo(label, fn)
  }

  /**
   * Gets the number of elements represented by this PCollection.
   *
   * @return The number of elements in this PCollection.
   */
  def length(): PObject[Long] = {
    setupRun()
    PObject(native.length())
  }

  /**
   * Returns a {@code PObject} containing the elements of this instance as a {@code Seq}.
   * @return
   */
  def asSeq(): PObject[Seq[S]] = {
    setupRun()
    PObject(native.asCollection())
  }

  /**
   * Returns the {@code PTypeFamily} of this instance.
   */
  def getTypeFamily() = {
    if (native.getTypeFamily == WritableTypeFamily.getInstance()) {
      Writables
    } else {
      Avros
    }
  }
}
