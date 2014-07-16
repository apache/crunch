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

import org.junit.Assert._
import org.junit.Test

class SecondarySortTest extends CrunchSuite with Serializable {

  @Test def testSecondarySortAvros {
    runSecondarySort(Avros)
  }

  @Test def testSecondarySortWritables {
    runSecondarySort(Writables)
  }

  def runSecondarySort(ptf: PTypeFamily) {
    val p = Pipeline.mapReduce(classOf[SecondarySortTest], tempDir.getDefaultConfiguration)
    val inputFile = tempDir.copyResourceFileName("secondary_sort_input.txt")
    val lines = p.read(From.textFile(inputFile, ptf.strings)).map(input => {
      val pieces = input.split(",")
      (pieces(0), (pieces(1).trim.toInt, pieces(2).trim.toInt))
    })
    .secondarySortAndMap((k, iter: Iterable[(Int, Int)]) => {
      List(k, iter.mkString(",")).mkString(",")
    })
    .materialize
    assertEquals(List("one,(-5,10),(1,1),(2,-3)", "three,(0,-1)", "two,(1,7),(2,6),(4,5)"), lines.toList)
    p.done
  }
}
