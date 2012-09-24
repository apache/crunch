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

import java.lang.{Boolean => JBoolean}
import java.lang.{Byte => JByte}
import java.lang.{Character => JCharacter}
import java.lang.{Double => JDouble}
import java.lang.{Float => JFloat}
import java.lang.{Integer => JInteger}
import java.lang.{Long => JLong}
import java.lang.{Short => JShort}
import java.util.{Collection => JCollection}
import java.util.{Map => JMap}

import org.apache.crunch.{PObject => JPObject}
import collection.JavaConversions

/**
 * Acts as a type class for conversions between two types.
 *
 * @tparam J The input type for the conversion.
 * @tparam T The output type for the conversion.
 */
trait Converter[J, T] {
  /**
   * Converts a value from the input type to the output type.
   *
   * @param j The value to convert.
   * @return The converted value.
   */
  def convert(j: J): T
}

/**
 * Companion object that encapsulates the members of the Converter type class.
 */
object Converter {

  /**
   * Converts Java Byte to Scala Byte.
   */
  implicit object ByteConverter extends Converter[JByte, Byte] {
    def convert(j: JByte) = Byte2byte(j)
  }

  /**
   * Converts Java Short to Scala Short.
   */
  implicit object ShortConverter extends Converter[JShort, Short] {
    def convert(j: JShort) = Short2short(j)
  }

  /**
   * Converts Java Integer to Scala Int.
   */
  implicit object IntegerConverter extends Converter[JInteger, Int] {
    def convert(j: JInteger) = Integer2int(j)
  }

  /**
   * Converts Java Long to Scala Long.
   */
  implicit object LongConverter extends Converter[JLong, Long] {
    def convert(j: JLong) = Long2long(j)
  }

  /**
   * Converts Java Float to Scala Float.
   */
  implicit object FloatConverter extends Converter[JFloat, Float] {
    def convert(j: JFloat) = Float2float(j)
  }

  /**
   * Converts Java Double to Scala Double.
   */
  implicit object DoubleConverter extends Converter[JDouble, Double] {
    def convert(j: JDouble) = Double2double(j)
  }

  /**
   * Converts Java Boolean to Scala Boolean.
   */
  implicit object BooleanConverter extends Converter[JBoolean, Boolean] {
    def convert(j: JBoolean) = Boolean2boolean(j)
  }

  /**
   * Converts Java Character to Scala Char.
   */
  implicit object CharacterConverter extends Converter[JCharacter, Char] {
    def convert(j: JCharacter) = Character2char(j)
  }

  /**
   * An implicit function that knows how to construct Converters from java.util.Collection[S]
   * to Seq[S].
   *
   * @tparam S The type contained in the collection to be converted.
   * @tparam T The type of collection.
   * @return A converter from the Collection[S] to the Seq[S].
   */
  implicit def collectionConverter[S, T <: JCollection[S]] =
    new Converter[JCollection[S], Seq[S]] {
      def convert(j: JCollection[S]) = JavaConversions.collectionAsScalaIterable(j).toSeq
    }

  /**
   * An implicit function that knows how to construct Converters from java.util.Map[K, V]
   * to Map[K, V].
   *
   * @tparam K The key type of the Map to be converted.
   * @tparam V The value type of the Map to be converted.
   * @tparam T The type of Map to be converted.
   * @return A converter from java.util.Map[K, V] to Map[K, V].
   */
  implicit def mapConverter[K, V, T <: JMap[K, V]] =
    new Converter[JMap[K, V], Map[K, V]] {
      def convert(j: JMap[K, V]) = JavaConversions.mapAsScalaMap(j).toMap
    }

  /**
   * An implicit function that knows how to construct identity Converters.
   *
   * @tparam T The type to be converted.
   * @return An identity converter.
   */
  implicit def identityConverter[T] = new Converter[T, T] {
    def convert(t: T) = t
  }
}

/**
 * A trait for classes that use a Converter.
 */
trait Convert {
  def convert[J, T](j: J)(implicit converter: Converter[J, T]): T = converter.convert(j)
}

/**
 * Represents a singleton value that results from a distributed computation.
 *
 * @tparam T The type of value encapsulated by this PObject.
 */
trait PObject[T] {
  /**
   * @return The value of this PObject.
   *
   */
  def value(): T
}

/**
 * An implementation of PObject whose value is obtained by applying a conversion to the value
 * obtained from a JPObject.
 *
 * @param native The backing Java PObject.
 * @param converter A converter from the Java value type to the Scala value type.
 * @tparam J The type of value encapsulated by the backing Java PObject.
 * @tparam T The type of value encapsulated by this Scala PObject.
 */
private[scrunch] class
    JPObjectWrapper[J, T](private val native: JPObject[J])(implicit val converter: Converter[J, T])
    extends PObject[T] with Convert {

  // Acts as a cache for the converted value.
  private var converted: Option[T] = None

  /**
   * Gets the value of this PObject by converting the value of the backing Java PObject.
   *
   * @return The value of this PObject.
   */
  override final def value(): T =
    converted.getOrElse{
      converted = Some(convert(native.getValue())(converter))
      converted.get
    }
}

/**
 * The companion object for PObject that provides factory methods for creating PObjects.
 */
private[scrunch] object PObject {
  /**
   * Creates a Scala PObject given a Java PObject.
   *
   * @param native The Java PObject to use when creating the Scala PObject.
   * @param converter A converter for the value type of the Java PObject to the value type of the
   *     Scala PObject.
   * @tparam J The type of value encapsulated by the Java PObject.
   * @tparam T The type of value encapsulated by the Scala PObject.
   * @return The Scala PObject.
   */
  def apply[J, T](native: JPObject[J])(implicit converter: Converter[J, T]): PObject[T] =
    new JPObjectWrapper[J, T](native)
}
