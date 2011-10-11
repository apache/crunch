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
package com.cloudera.crunch.type.writables;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import com.cloudera.crunch.type.writable.WritableType;
import com.cloudera.crunch.type.writable.Writables;

public class WritablesTest {

  @Test
  public void testBytes() throws Exception {
    byte[] bytes = new byte[] { 17, 26, -98 };
    BytesWritable bw = new BytesWritable(bytes);
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    WritableType<ByteBuffer, BytesWritable> ptype = Writables.bytes();
    assertEquals(bb, ptype.getDataBridge().getInputMapFn().map(bw));
    assertEquals(bw, ptype.getDataBridge().getOutputMapFn().map(bb));
  }
}
