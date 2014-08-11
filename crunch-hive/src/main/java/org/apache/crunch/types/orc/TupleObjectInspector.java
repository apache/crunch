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
package org.apache.crunch.types.orc;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.crunch.Tuple;
import org.apache.crunch.Union;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.TupleFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;

/**
 * An object inspector to define the structure of Crunch Tuples
 *
 */
public class TupleObjectInspector<T extends Tuple> extends StructObjectInspector {
  
  private TupleFactory<T> tupleFactory;
  private List<TupleField> fields;
  
  public TupleObjectInspector(TupleFactory<T> tupleFactory, PType... ptypes) {
    this.tupleFactory = tupleFactory;
    fields = new ArrayList<TupleField>();
    for (int i = 0; i < ptypes.length; i++) {
      TupleField field = new TupleField(i, ptypes[i]);
      fields.add(field);
    }
  }
  
  static class TupleField implements StructField {
    
    private int index;
    private ObjectInspector oi;
    
    public TupleField(int index, PType<?> ptype) {
      this.index = index;
      oi = createObjectInspector(ptype);
    }
    
    private ObjectInspector createObjectInspector(PType<?> ptype) {
      Class typeClass = ptype.getTypeClass();
      if (typeClass == Union.class || typeClass == Void.class) {
        throw new IllegalArgumentException(typeClass.getName() + " is not supported yet");
      }
      
      ObjectInspector result;
      if (typeClass == ByteBuffer.class) {
        result = new ByteBufferObjectInspector();
      } else if (typeClass == Collection.class) {
        ObjectInspector itemOi = createObjectInspector(ptype.getSubTypes().get(0));
        result = ObjectInspectorFactory.getStandardListObjectInspector(itemOi);
      } else if (typeClass == Map.class) {
        ObjectInspector keyOi = ObjectInspectorFactory
            .getReflectionObjectInspector(String.class, ObjectInspectorOptions.JAVA);
        ObjectInspector valueOi = createObjectInspector(ptype.getSubTypes().get(0));
        result = ObjectInspectorFactory.getStandardMapObjectInspector(keyOi, valueOi);
      } else if (Tuple.class.isAssignableFrom(typeClass)) {
        result = new TupleObjectInspector(TupleFactory.getTupleFactory(typeClass),
            ptype.getSubTypes().toArray(new PType[0]));
      } else {
        result = ObjectInspectorFactory.getReflectionObjectInspector(typeClass,
            ObjectInspectorOptions.JAVA);
      }
      return result;
    }

    @Override
    public String getFieldName() {
      return "_col" + index;
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return oi;
    }

    @Override
    public String getFieldComment() {
      return null;
    }
    
  }

  @Override
  public String getTypeName() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("struct<");
    for (int i = 0; i < fields.size(); ++i) {
        StructField field = fields.get(i);
        if (i != 0) {
            buffer.append(",");
        }
        buffer.append(field.getFieldName());
        buffer.append(":");
        buffer.append(field.getFieldObjectInspector().getTypeName());
    }
    buffer.append(">");
    return buffer.toString();
  }

  @Override
  public Category getCategory() {
    return Category.STRUCT;
  }
  
  public T create(Object... values) {
    return tupleFactory.makeTuple(values);
  }

  @Override
  public List<? extends StructField> getAllStructFieldRefs() {
    return fields;
  }

  @Override
  public StructField getStructFieldRef(String fieldName) {
    for (StructField field : fields) {
      if (field.getFieldName().equals(fieldName)) {
        return field;
      }
    }
    return null;
  }
  
  @Override
  public Object getStructFieldData(Object data, StructField fieldRef) {
    TupleField field = (TupleField) fieldRef;
    return ((T) data).get(field.index);
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object data) {
    T tuple = (T) data;
    List<Object> result = new ArrayList<Object>();
    for (int i = 0; i < tuple.size(); i++) {
      result.add(tuple.get(i));
    }
    return result;
  }
  
  
  static class ByteBufferObjectInspector extends AbstractPrimitiveJavaObjectInspector implements SettableBinaryObjectInspector {

    ByteBufferObjectInspector() {
      super(TypeInfoFactory.binaryTypeInfo);
    }
    
    @Override
    public ByteBuffer copyObject(Object o) {
      if (o == null) {
        return null;
      }
      byte[] oldBytes = getPrimitiveJavaObject(o);
      byte[] copiedBytes = new byte[oldBytes.length];
      System.arraycopy(oldBytes, 0, copiedBytes, 0, oldBytes.length);
      ByteBuffer duplicate = ByteBuffer.wrap(copiedBytes);
      return duplicate;
    }
    
    @Override
    public BytesWritable getPrimitiveWritableObject(Object o) {
      if (o == null) {
        return null;
      }
      ByteBuffer buf = (ByteBuffer) o;
      BytesWritable bw = new BytesWritable();
      bw.set(buf.array(), buf.arrayOffset(), buf.limit());
      return bw;
    }
    
    @Override
    public byte[] getPrimitiveJavaObject(Object o) {
      if (o == null) {
        return null;
      }
      ByteBuffer buf = (ByteBuffer) o;
      byte[] b = new byte[buf.limit()];
      System.arraycopy(buf.array(), buf.arrayOffset(), b, 0, b.length);
      return b;
    }

    @Override
    public Object set(Object o, byte[] b) {
      throw new UnsupportedOperationException("set is not supported");
    }

    @Override
    public Object set(Object o, BytesWritable bw) {
      throw new UnsupportedOperationException("set is not supported");
    }

    @Override
    public ByteBuffer create(byte[] bb) {
      return bb == null ? null : ByteBuffer.wrap(bb);
    }

    @Override
    public ByteBuffer create(BytesWritable bw) {
      return bw == null ? null : ByteBuffer.wrap(bw.getBytes());
    }

    
  }

}
