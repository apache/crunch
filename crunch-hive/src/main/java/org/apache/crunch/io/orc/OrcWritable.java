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
package org.apache.crunch.io.orc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.types.orc.OrcUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;

public class OrcWritable implements WritableComparable<OrcWritable> {

  private OrcStruct orc;
  private ObjectInspector oi; // object inspector for orc struct

  private BytesWritable blob; // serialized from orc struct
  private BinarySortableSerDe serde;

  @Override
  public void write(DataOutput out) throws IOException {
    serialize();
    blob.write(out);
  }

  private void serialize() {
    try {
      if (blob == null) {
        // Make a copy since BinarySortableSerDe will reuse the byte buffer.
        // This is not very efficient for the current implementation. Shall we
        // implement a no-reuse version of BinarySortableSerDe?
        byte[] bytes = ((BytesWritable) serde.serialize(orc, oi)).getBytes();
        byte[] newBytes = new byte[bytes.length];
        System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
        blob = new BytesWritable(newBytes);
      }
    } catch (SerDeException e) {
      throw new CrunchRuntimeException("Unable to serialize object: "
          + orc);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    blob = new BytesWritable();
    blob.readFields(in);
    orc = null; // the orc struct is stale
  }

  @Override
  public int compareTo(OrcWritable arg0) {
    serialize();
    arg0.serialize();
    return ((Comparable) blob).compareTo((Comparable) arg0.blob);
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    return compareTo((OrcWritable) obj) == 0;
  }

  public void setSerde(BinarySortableSerDe serde) {
    this.serde = serde;
  }

  public void setObjectInspector(ObjectInspector oi) {
    this.oi = oi;
  }
  
  public ObjectInspector getObjectInspector() {
    return oi;
  }

  public void set(OrcStruct orcStruct) {
    this.orc = orcStruct;
    blob = null; // the blob is stale
  }
  
  public OrcStruct get() {
    if (orc == null && blob != null) {
      makeOrcStruct();
    }
    return orc;
  }
  
  private void makeOrcStruct() {
    try {
      Object row = serde.deserialize(blob);
      StructObjectInspector rowOi = (StructObjectInspector) serde.getObjectInspector();
      orc = (OrcStruct) OrcUtils.convert(row, rowOi, oi);
    } catch (SerDeException e) {
      throw new CrunchRuntimeException("Unable to deserialize blob: " + blob);
    }
  }
  
}
