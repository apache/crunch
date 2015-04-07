/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.crunch.scrunch

import org.apache.crunch.{FilterFn, Pair => CPair}

/**
 * The {@code Incrementable[T]} trait defines an object that allows a counter to
 * be incremented and then returns a reference to another object of the same type.
 * Both the {@link PCollection} and {@link PTable} types in Scrunch support the
 * {@code Incrementable} trait.
 */
trait Incrementable[T] {

  private val enumCache = new java.util.HashMap[Enumeration, String]()

  def increment(counter: Enum[_]): T = {
    increment(counter, 1)
  }

  def increment(counter: Enum[_], count: Long): T = {
    increment(counter.getClass.getCanonicalName, counter.toString, count)
  }

  def increment(groupEnum: Enumeration, value: Enumeration#Value): T = {
    increment(groupEnum, value, 1)
  }

  def increment(groupEnum: Enumeration, value: Enumeration#Value, count: Long): T = {
    var groupName = enumCache.get(groupEnum)
    if (groupName == null) {
      groupName = groupEnum.toString
      enumCache.put(groupEnum, groupName)
    }
    increment(groupName, value.toString, count)
  }

  def increment(groupName: String, counterName: String): T = {
    increment(groupName, counterName, 1)
  }

  def increment(groupName: String, counterName: String, count: Long): T
}

/**
 * Incrementable classes may also support conditionally incrementing a counter,
 * such as via the {@link PCollection#incrementIf} method or the {@link PTable#incrementIf}
 * and {@link PTable#incrementIfValue} methods. In these cases, the return type
 * is an instance of {@code Increment} that returns a reference to a new PCollection/PTable
 * after it is applied to a specified counter group and value.
 */
trait Increment[T] {

  private val enumCache = new java.util.HashMap[Enumeration, String]()

  def apply(counter: Enum[_]): T = {
    apply(counter, 1)
  }

  def apply(counter: Enum[_], count: Long): T = {
    apply(counter.getClass.getCanonicalName, counter.toString, count)
  }

  def apply(groupEnum: Enumeration, value: Enumeration#Value): T = {
    apply(groupEnum, value, 1)
  }

  def apply(groupEnum: Enumeration, value: Enumeration#Value, count: Long): T = {
    var groupName = enumCache.get(groupEnum)
    if (groupName == null) {
      groupName = groupEnum.toString
      enumCache.put(groupEnum, groupName)
    }
    apply(groupName, value.toString, count)
  }

  def apply(groupName: String, counterName: String): T = {
    apply(groupName, counterName, 1)
  }

  def apply(groupName: String, counterName: String, count: Long): T
}

class IncrementPCollection[S](val pc: PCollection[S]) extends Increment[PCollection[S]] {
  override def apply(groupName: String, counterName: String, count: Long) = {
    pc.parallelDo("inc=" + groupName + ":" + counterName,
      new CounterFn[S](groupName, counterName, count),
      pc.pType())
  }
}

class IncrementIfPCollection[S](val pc: PCollection[S], val f: S => Boolean) extends Increment[PCollection[S]] {
  override def apply(groupName: String, counterName: String, count: Long) = {
    pc.parallelDo("incif=" + groupName + ":" + counterName,
      new IfCounterFn[S](groupName, counterName, count, f),
      pc.pType())
  }
}

class IncrementPTable[K, V](val pc: PTable[K, V]) extends Increment[PTable[K, V]] {
  override def apply(groupName: String, counterName: String, count: Long) = {
    pc.parallelDo("inc=" + groupName + ":" + counterName,
      new CounterFn[CPair[K, V]](groupName, counterName, count),
      pc.pType())
  }
}

class IncrementIfPTable[K, V](val pc: PTable[K, V], val f: CPair[K, V] => Boolean) extends Increment[PTable[K, V]] {
  override def apply(groupName: String, counterName: String, count: Long) = {
    pc.parallelDo("inc=" + groupName + ":" + counterName,
      new IfCounterFn[CPair[K, V]](groupName, counterName, count, f),
      pc.pType())
  }
}

class CounterFn[S](val group: String, val counter: String, val count: Long)
  extends FilterFn[S] {
  override def scaleFactor() = 1.0f

  def accept(s: S) = {
    increment(group, counter, count)
    true
  }
}

class IfCounterFn[S](val group: String, val counter: String, val count: Long, val cond: S => Boolean)
  extends FilterFn[S] {
  override def scaleFactor() = 1.0f

  def accept(s: S) = {
    if (cond(s)) {
      increment(group, counter, count)
    }
    true
  }
}

