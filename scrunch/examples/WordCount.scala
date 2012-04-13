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
import com.cloudera.scrunch.PipelineApp

object WordCount extends PipelineApp {

  def countWords(file: String) = {
    read(from.textFile(file))
      .flatMap(_.split("\\W+").filter(!_.isEmpty()))
      .count
  }

  val counts = join(countWords(args(0)), countWords(args(1)))
  write(counts, to.textFile(args(2)))
}
