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

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.crunch.CrunchRuntimeException;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class OrcUtils {
  
  /**
   * Generate TypeInfo for a given java class based on reflection
   * 
   * @param typeClass
   * @return
   */
  public static TypeInfo getTypeInfo(Class<?> typeClass) {
    ObjectInspector oi = ObjectInspectorFactory
        .getReflectionObjectInspector(typeClass, ObjectInspectorOptions.JAVA);
    return TypeInfoUtils.getTypeInfoFromObjectInspector(oi);
  }
  
  /**
   * Create an object of OrcStruct given a type string and a list of objects
   * 
   * @param typeInfo
   * @param objs
   * @return
   */
  public static OrcStruct createOrcStruct(TypeInfo typeInfo, Object... objs) {
    SettableStructObjectInspector oi = (SettableStructObjectInspector) OrcStruct
        .createObjectInspector(typeInfo);
    List<StructField> fields = (List<StructField>) oi.getAllStructFieldRefs();
    OrcStruct result = (OrcStruct) oi.create();
    result.setNumFields(fields.size());
    for (int i = 0; i < fields.size(); i++) {
      oi.setStructFieldData(result, fields.get(i), objs[i]);
    }
    return result;
  }
  
  /**
   * Create a binary serde for OrcStruct serialization/deserialization
   * 
   * @param typeInfo
   * @return
   */
  public static BinarySortableSerDe createBinarySerde(TypeInfo typeInfo){
    BinarySortableSerDe serde = new BinarySortableSerDe();
    
    StringBuffer nameSb = new StringBuffer();
    StringBuffer typeSb = new StringBuffer();

    StructTypeInfo sti = (StructTypeInfo) typeInfo;
    for (String name : sti.getAllStructFieldNames()) {
      nameSb.append(name);
      nameSb.append(',');
    }
    for (TypeInfo info : sti.getAllStructFieldTypeInfos()) {
      typeSb.append(info.toString());
      typeSb.append(',');
    }

    Properties tbl = new Properties();
    String names = nameSb.length() > 0 ? nameSb.substring(0,
        nameSb.length() - 1) : "";
    String types = typeSb.length() > 0 ? typeSb.substring(0,
        typeSb.length() - 1) : "";
    tbl.setProperty(serdeConstants.LIST_COLUMNS, names);
    tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES, types);
    
    try {
      serde.initialize(null, tbl);
    } catch (SerDeException e) {
      throw new CrunchRuntimeException("Unable to initialize binary serde");
    }
    
    return serde;
  }
  
  /**
   * Convert an object from / to OrcStruct
   * 
   * @param from
   * @param fromOi
   * @param toOi
   * @return
   */
  public static Object convert(Object from, ObjectInspector fromOi, ObjectInspector toOi) {
    if (from == null) {
      return null;
    }
    Object to;
    switch (fromOi.getCategory()) {
    case PRIMITIVE:
      PrimitiveObjectInspector fromPoi = (PrimitiveObjectInspector) fromOi;
      switch (fromPoi.getPrimitiveCategory()) {
      case FLOAT:
        SettableFloatObjectInspector floatOi = (SettableFloatObjectInspector) toOi;
        return floatOi.create((Float) fromPoi.getPrimitiveJavaObject(from));
      case DOUBLE:
        SettableDoubleObjectInspector doubleOi = (SettableDoubleObjectInspector) toOi;
        return doubleOi.create((Double) fromPoi.getPrimitiveJavaObject(from));
      case BOOLEAN:
        SettableBooleanObjectInspector boolOi = (SettableBooleanObjectInspector) toOi;
        return boolOi.create((Boolean) fromPoi.getPrimitiveJavaObject(from));
      case INT:
        SettableIntObjectInspector intOi = (SettableIntObjectInspector) toOi;
        return intOi.create((Integer) fromPoi.getPrimitiveJavaObject(from));
      case LONG:
        SettableLongObjectInspector longOi = (SettableLongObjectInspector) toOi;
        return longOi.create((Long) fromPoi.getPrimitiveJavaObject(from));
      case STRING:
        SettableStringObjectInspector strOi = (SettableStringObjectInspector) toOi;
        return strOi.create((String) fromPoi.getPrimitiveJavaObject(from));
      case BYTE:
        SettableByteObjectInspector byteOi = (SettableByteObjectInspector) toOi;
        return byteOi.create((Byte) fromPoi.getPrimitiveJavaObject(from));
      case SHORT:
        SettableShortObjectInspector shortOi = (SettableShortObjectInspector) toOi;
        return shortOi.create((Short) fromPoi.getPrimitiveJavaObject(from));
      case BINARY:
        SettableBinaryObjectInspector binOi = (SettableBinaryObjectInspector) toOi;
        return binOi.create((byte[]) fromPoi.getPrimitiveJavaObject(from));
      case TIMESTAMP:
        SettableTimestampObjectInspector timeOi = (SettableTimestampObjectInspector) toOi;
        return timeOi.create((Timestamp) fromPoi.getPrimitiveJavaObject(from));
      case DATE:
        SettableDateObjectInspector dateOi = (SettableDateObjectInspector) toOi;
        return dateOi.create((Date) fromPoi.getPrimitiveJavaObject(from));
      case DECIMAL:
        SettableHiveDecimalObjectInspector decimalOi = (SettableHiveDecimalObjectInspector) toOi;
        return decimalOi.create((HiveDecimal) fromPoi.getPrimitiveJavaObject(from));
      case CHAR:
        SettableHiveCharObjectInspector charOi = (SettableHiveCharObjectInspector) toOi;
        return charOi.create((HiveChar) fromPoi.getPrimitiveJavaObject(from));
      case VARCHAR:
        SettableHiveVarcharObjectInspector varcharOi = (SettableHiveVarcharObjectInspector) toOi;
        return varcharOi.create((HiveVarchar) fromPoi.getPrimitiveJavaObject(from));
      case VOID:
        throw new IllegalArgumentException("Void type is not supported yet");
      default:
        throw new IllegalArgumentException("Unknown primitive type "
            + (fromPoi).getPrimitiveCategory());
      }
    case STRUCT:
      StructObjectInspector fromSoi = (StructObjectInspector) fromOi;
      List<StructField> fromFields = (List<StructField>) fromSoi.getAllStructFieldRefs();
      List<Object> fromItems = fromSoi.getStructFieldsDataAsList(from);
      
      // this is a tuple. use TupleObjectInspector to construct the result
      if (toOi instanceof TupleObjectInspector) {
        TupleObjectInspector toToi = (TupleObjectInspector) toOi;
        List<StructField> toFields = (List<StructField>) toToi.getAllStructFieldRefs();
        Object[] values = new Object[fromItems.size()];
        for (int i = 0; i < fromItems.size(); i++) {
          values[i] = convert(fromItems.get(i),
            fromFields.get(i).getFieldObjectInspector(),
            toFields.get(i).getFieldObjectInspector());
        }
        return toToi.create(values);
      }
      
      SettableStructObjectInspector toSoi = (SettableStructObjectInspector) toOi;
      List<StructField> toFields = (List<StructField>) toSoi.getAllStructFieldRefs();
      to = toSoi.create();
      for (int i = 0; i < fromItems.size(); i++) {
        Object converted = convert(fromItems.get(i),
            fromFields.get(i).getFieldObjectInspector(),
            toFields.get(i).getFieldObjectInspector());
        toSoi.setStructFieldData(to, toFields.get(i), converted);
      }
      return to;
    case MAP:
      MapObjectInspector fromMoi = (MapObjectInspector) fromOi;
      SettableMapObjectInspector toMoi = (SettableMapObjectInspector) toOi;
      to = toMoi.create(); // do not reuse
      for (Map.Entry<?, ?> entry : fromMoi.getMap(from).entrySet()) {
        Object convertedKey = convert(entry.getKey(),
            fromMoi.getMapKeyObjectInspector(),
            toMoi.getMapKeyObjectInspector());
        Object convertedValue = convert(entry.getValue(),
            fromMoi.getMapValueObjectInspector(),
            toMoi.getMapValueObjectInspector());
        toMoi.put(to, convertedKey, convertedValue);
      }
      return to;
    case LIST:
      ListObjectInspector fromLoi = (ListObjectInspector) fromOi;
      List<?> fromList = fromLoi.getList(from);
      
      SettableListObjectInspector toLoi = (SettableListObjectInspector) toOi;
      to = toLoi.create(fromList.size()); // do not reuse
      for (int i = 0; i < fromList.size(); i++) {
        Object converted = convert(fromList.get(i),
            fromLoi.getListElementObjectInspector(),
            toLoi.getListElementObjectInspector());
        toLoi.set(to, i, converted);
      }
      return to;
    case UNION:
      throw new IllegalArgumentException("Union type is not supported yet");
    default:
      throw new IllegalArgumentException("Unknown type " + fromOi.getCategory());
    }
  }

}
