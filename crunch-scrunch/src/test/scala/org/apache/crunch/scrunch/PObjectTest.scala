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
import java.util.{ArrayList => JArrayList}
import java.util.{HashMap => JHashMap}
import java.util.{Collection => JCollection}
import java.util.{Map => JMap}

import org.apache.crunch.{PObject => JPObject}
import org.junit.Assert._
import org.junit.Test
import org.scalatest.junit.JUnitSuite

/**
 * Provides tests for converting Java PObjects to Scala PObjects.
 */
class PObjectTest extends JUnitSuite {

  /**
   * Constructs a Java PObject that returns the specified value.
   *
   * @param value The value the Java PObject should return.
   * @tparam T The type of the value.
   * @return A Java PObject encapsulating the specified value.
   */
  private def getJPObject[T](value: T): JPObject[T] = {
    new JPObject[T] {
      override def getValue: T = value
    }
  }

  /**
   * Tests converting a JPObject containing a JBoolean to a Scala PObject containing a Boolean.
   */
  @Test def testJBoolean2Boolean() {
    val value: Boolean = PObject[JBoolean, Boolean](getJPObject(JBoolean.TRUE)).value()
    assertEquals("Wrong value for PObject of Boolean", true, value)
  }

  /**
   * Tests converting a JPObject containing a JByte to a Scala PObject containing a Byte.
   */
  @Test def testJByte2Byte() {
    val value: Byte = PObject[JByte, Byte](getJPObject(JByte.MAX_VALUE)).value()
    assertEquals("Wrong value for PObject of Byte", Byte.MaxValue, value)
  }

  /**
   * Tests converting a JPObject containing a JCharacter to a Scala PObject containing a Char.
   */
  @Test def testJCharacter2Char() {
    val value: Char = PObject[JCharacter, Char](getJPObject(new JCharacter('c'))).value()
    assertEquals("Wrong value for PObject of Character", 'c', value)
  }

  /**
   * Tests converting a JPObject containing a JDouble to a Scala PObject containing a Double.
   */
  @Test def testJDouble2Double() {
    val value: Double = PObject[JDouble, Double](getJPObject(new JDouble(5.0))).value()
    assertEquals("Wrong value for PObject of Double", 5.0D, value, 0.0D)
  }

  /**
   * Tests converting a JPObject containing a JFloat to a Scala PObject containing a Float.
   */
  @Test def testJFloat2Float() {
    val value: Float = PObject[JFloat, Float](getJPObject(new JFloat(5.0))).value()
    assertEquals("Wrong value for PObject of Float", 5.0F, value, 0.0F)
  }

  /**
   * Tests converting a JPObject containing a JShort to a Scala PObject containing a Short.
   */
  @Test def testJShort2Short() {
    val value: Short = PObject[JShort, Short](getJPObject(JShort.MAX_VALUE)).value()
    assertEquals("Wrong value for PObject of Short", Short.MaxValue, value)
  }

  /**
   * Tests converting a JPObject containing a JInteger to a Scala PObject containing a Int.
   */
  @Test def testJInt2Int() {
    val value: Int = PObject[JInteger, Int](getJPObject(new JInteger(5))).value()
    assertEquals("Wrong value for PObject of Integer", 5, value)
  }

  /**
   * Tests converting a JPObject containing a JLong to a Scala PObject containing a Long.
   */
  @Test def testJLong2Long() {
    val value: Long = PObject[JLong, Long](getJPObject(new JLong(5L))).value()
    assertEquals("Wrong value for PObject of Long", 5L, value)
  }

  /**
   * Tests converting a JPObject containing a JCollection to a Scala PObject containing a Seq.
   */
  @Test def testJCollection2Seq() {
    val list = new JArrayList[Int]()
    list.add(1)
    list.add(2)
    list.add(3)

    val jCollect: JCollection[Int] = list
    val value: Seq[Int] = PObject[JCollection[Int], Seq[Int]](getJPObject(jCollect)).value()
    assertEquals("Wrong size for PObject of Seq", 3, value.length)
    assertTrue("PObject of Seq contains wrong elements.", value.contains(1))
    assertTrue("PObject of Seq contains wrong elements.", value.contains(2))
    assertTrue("PObject of Seq contains wrong elements.", value.contains(3))
  }

  /**
   * Tests converting a JPObject containing a JMap to a Scala PObject containing a Map.
   */
  @Test def testJMap2Map() {
    val hashMap = new JHashMap[String, Int]()
    hashMap.put("hello", 1)
    hashMap.put("beautiful", 2)
    hashMap.put("world", 3)

    val jMap: JMap[String, Int] = hashMap
    val value: Map[String, Int] =
        PObject[JMap[String, Int], Map[String, Int]](getJPObject(jMap)).value()
    assertEquals("Wrong size for PObject of Map", 3, value.size)
    assertTrue("PObject of Map contains wrong keys.", value.contains("hello"))
    assertTrue("PObject of Map contains wrong keys.", value.contains("beautiful"))
    assertTrue("PObject of Map contains wrong keys.", value.contains("world"))
    assertEquals("PObject of Map contains wrong values.", 1, value("hello"))
    assertEquals("PObject of Map contains wrong values.", 2, value("beautiful"))
    assertEquals("PObject of Map contains wrong values.", 3, value("world"))
  }
}
