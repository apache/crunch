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
package org.apache.crunch.types.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import com.google.common.collect.Lists;

/**
 * Determines the capabilities of the Avro version that is currently being used.
 */
class AvroCapabilities {

  public static class Record extends org.apache.avro.specific.SpecificRecordBase implements
      org.apache.avro.specific.SpecificRecord {
    public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser()
        .parse("{\"type\":\"record\",\"name\":\"Record\",\"namespace\":\"org.apache.crunch.types.avro\",\"fields\":[{\"name\":\"subrecords\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]}");
    @Deprecated
    public java.util.List<java.lang.CharSequence> subrecords;

    public java.lang.Object get(int field$) {
      switch (field$) {
      case 0:
        return subrecords;
      default:
        throw new org.apache.avro.AvroRuntimeException("Bad index");
      }
    }

    // Used by DatumReader. Applications should not call.
    @SuppressWarnings(value = "unchecked")
    public void put(int field$, java.lang.Object value$) {
      switch (field$) {
      case 0:
        subrecords = (java.util.List<java.lang.CharSequence>) value$;
        break;
      default:
        throw new org.apache.avro.AvroRuntimeException("Bad index");
      }
    }

    @Override
    public Schema getSchema() {
      return SCHEMA$;
    }
  }

  /**
   * Determine if the current Avro version can use the ReflectDatumReader to
   * read SpecificData that includes an array. The inability to do this was a
   * bug that was fixed in Avro 1.7.0.
   * 
   * @return true if SpecificData can be properly read using a
   *         ReflectDatumReader
   */
  static boolean canDecodeSpecificSchemaWithReflectDatumReader() {
    ReflectDatumReader<Record> datumReader = new ReflectDatumReader(Record.SCHEMA$);
    ReflectDatumWriter<Record> datumWriter = new ReflectDatumWriter(Record.SCHEMA$);

    Record record = new Record();
    record.subrecords = Lists.<CharSequence> newArrayList("a", "b");

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);

    try {
      datumWriter.write(record, encoder);
      encoder.flush();
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(
          byteArrayOutputStream.toByteArray(), null);
      datumReader.read(record, decoder);
    } catch (IOException ioe) {
      throw new RuntimeException("Error performing specific schema test", ioe);
    } catch (ClassCastException cce) {
      // This indicates that we're using a pre-1.7.0 version of Avro, as the
      // ReflectDatumReader in those versions could not correctly handle an
      // array in a SpecificData value
      return false;
    }
    return true;
  }
}
