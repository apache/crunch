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

import com.cloudera.crunch.test.FileHelper
import com.cloudera.scrunch.PipelineApp

import org.junit.Test

class PipelineAppTest {

  object WordCount extends PipelineApp {
    read(from.textFile(args(0)))
        .flatMap(_.split("\\W+").filter(!_.isEmpty()))
        .count
        .write(to.textFile(args(1)))
    done
  }

  @Test def run {
    val args = new Array[String](2)
    args(0) = FileHelper.createTempCopyOf("shakes.txt")
    args(1) = FileHelper.createOutputPath.getAbsolutePath
    WordCount.main(args)
  }
}
