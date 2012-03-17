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
package com.cloudera.crunch.type.avro;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectDatumReader;

public class SafeReflectDatumReader<T> extends ReflectDatumReader<T> {
  /** Construct where the writer's and reader's schemas are the same. */
  public SafeReflectDatumReader(Schema root) {
    super(root, root, Avros.REFLECT_DATA_INSTANCE);
  }

  /** Construct given writer's and reader's schema. */
  public SafeReflectDatumReader(Schema writer, Schema reader) {
    super(writer, reader, Avros.REFLECT_DATA_INSTANCE);
  }
}
