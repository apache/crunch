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
package org.apache.crunch.impl.spark;

import com.google.common.primitives.UnsignedBytes;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryData;

import java.io.Serializable;
import java.util.Arrays;

public abstract class ByteArrayHelper implements Serializable {

  public static final ByteArrayHelper WRITABLES = new ByteArrayHelper() {
    @Override
    boolean equal(byte[] left, byte[] right) {
      return Arrays.equals(left, right);
    }

    @Override
    int hashCode(byte[] value) {
      return value != null ? Arrays.hashCode(value) : 0;
    }

    @Override
    int compare(byte[] left, byte[] right) {
      return UnsignedBytes.lexicographicalComparator().compare(left, right);
    }
  };

  public static ByteArrayHelper forAvroSchema(Schema schema) {
    return new AvroByteArrayHelper(schema);
  }

  abstract boolean equal(byte[] left, byte[] right);
  abstract int hashCode(byte[] value);
  abstract int compare(byte[] left, byte[] right); 

  static class AvroByteArrayHelper extends ByteArrayHelper {
    private String jsonSchema;
    private transient Schema schema;

    public AvroByteArrayHelper(Schema schema) {
      this.jsonSchema = schema.toString();
    }

    private Schema getSchema() {
      if (schema == null) {
        schema = new Schema.Parser().parse(jsonSchema);
      }
      return schema;
    }

    @Override 
    boolean equal(byte[] left, byte[] right) {
      return compare(left, right) == 0;
    }

    @Override
    int hashCode(byte[] value) {
      return BinaryData.hashCode(value, 0, value.length, getSchema());
    }

    @Override
    int compare(byte[] left, byte[] right) {
      return BinaryData.compare(left, 0, right, 0, getSchema());
    }
  }
}
