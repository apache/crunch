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
package org.apache.crunch.lib.sort;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryData;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.reflect.ReflectData;
import org.apache.crunch.types.avro.AvroMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.RawComparator;

public class ReverseAvroComparator<T> extends Configured implements RawComparator<AvroKey<T>> {

  private Schema schema;
  private AvroMode mode;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf != null) {
      schema = (new Schema.Parser()).parse(conf.get("crunch.schema"));
      mode = AvroMode.fromShuffleConfiguration(conf);
    }
  }

  @Override
  public int compare(AvroKey<T> o1, AvroKey<T> o2) {
    return mode.getData().compare(o2.datum(), o1.datum(), schema);
  }

  @Override
  public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5) {
    return BinaryData.compare(arg3, arg4, arg5, arg0, arg1, arg2, schema);
  }
}