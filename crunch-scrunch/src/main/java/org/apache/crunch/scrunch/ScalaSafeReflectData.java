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
package org.apache.crunch.scrunch;

import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.Stringable;
import org.apache.avro.reflect.Union;
import org.apache.avro.specific.FixedSize;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.NullNode;

/**
 * Scala-oriented support class for serialization via reflection.
 */
public class ScalaSafeReflectData extends ReflectData.AllowNull {

  private static final ScalaSafeReflectData INSTANCE = new ScalaSafeReflectData();
  
  public static ScalaSafeReflectData getInstance() { return INSTANCE; }
  
  static final String CLASS_PROP = "java-class";
  static final String ELEMENT_PROP = "java-element-class";
  
  static Class getClassProp(Schema schema, String prop) {
    String name = schema.getProp(prop);
    if (name == null) return null;
    try {
      return Class.forName(name);
    } catch (ClassNotFoundException e) {
      throw new AvroRuntimeException(e);
    }
  }
  
  /**
   * This method is the whole reason for this class to exist, so that I can
   * hack around a problem where calling getSimpleName on a class that is
   * defined inside of the Scala REPL can cause an internal language error,
   * which I'm not a huge fan of.
   * 
   * @param clazz
   * @return
   */
  private String getSimpleName(Class clazz) {
    try {
      return clean(clazz.getSimpleName());
    } catch (InternalError ie) {
      // This can happen in Scala when we're using the Console. Crazy, right?
      String fullName = clazz.getName();
      String[] pieces = fullName.split("\\.");
      return clean(pieces[pieces.length - 1]);
    }
  }
  
  @Override
  @SuppressWarnings(value="unchecked")
  protected Schema createSchema(Type type, Map<String,Schema> names) {
    if (type instanceof GenericArrayType) {                  // generic array
      Type component = ((GenericArrayType)type).getGenericComponentType();
      if (component == Byte.TYPE)                           // byte array
        return Schema.create(Schema.Type.BYTES);           
      Schema result = Schema.createArray(createSchema(component, names));
      setElement(result, component);
      return result;
    } else if (type instanceof ParameterizedType) {
      ParameterizedType ptype = (ParameterizedType)type;
      Class raw = (Class)ptype.getRawType();
      Type[] params = ptype.getActualTypeArguments();
      if (java.util.Map.class.isAssignableFrom(raw) ||
          scala.collection.Map.class.isAssignableFrom(raw)) {
        Type key = params[0];
        Type value = params[1];
        if (!(key == String.class))
          throw new AvroTypeException("Map key class not String: "+key);
        Schema schema = Schema.createMap(createSchema(value, names));
        schema.addProp(CLASS_PROP, raw.getName());
        return schema;
      } else if (Collection.class.isAssignableFrom(raw) ||
          scala.collection.Iterable.class.isAssignableFrom(raw)) {   // Collection
        if (params.length != 1)
          throw new AvroTypeException("No array type specified.");
        Schema schema = Schema.createArray(createSchema(params[0], names));
        schema.addProp(CLASS_PROP, raw.getName());
        return schema;
      } else {
        throw new AvroTypeException("Could not convert type: " + type);
      }
    } else if ((type == Short.class) || (type == Short.TYPE)) {
      Schema result = Schema.create(Schema.Type.INT);
      result.addProp(CLASS_PROP, Short.class.getName());
      return result;
    } else if (type instanceof Class) {                      // Class
      Class<?> c = (Class<?>)type;
      if (c.isPrimitive() || Number.class.isAssignableFrom(c)
          || c == Void.class || c == Boolean.class)          // primitive
        return super.createSchema(type, names);
      if (c.isArray()) {                                     // array
        Class component = c.getComponentType();
        if (component == Byte.TYPE) {                        // byte array
          Schema result = Schema.create(Schema.Type.BYTES);
          result.addProp(CLASS_PROP, c.getName()); // For scala-specific byte arrays
          return result;
        }
        Schema result = Schema.createArray(createSchema(component, names));
        result.addProp(CLASS_PROP, c.getName());
        result.addProp(ELEMENT_PROP, component.getName());
        setElement(result, component);
        return result;
      }
      if (CharSequence.class.isAssignableFrom(c))            // String
        return Schema.create(Schema.Type.STRING);
      if (ByteBuffer.class.isAssignableFrom(c)) {
        return Schema.create(Schema.Type.BYTES);
      }
      String fullName = c.getName();
      Schema schema = names.get(fullName);
      if (schema == null) {
        String name = getSimpleName(c);
        String space = c.getPackage() == null ? "" : c.getPackage().getName();
        if (c.getEnclosingClass() != null)                   // nested class
          space = c.getEnclosingClass().getName() + "$";
        Union union = c.getAnnotation(Union.class);
        if (union != null) {                                 // union annotated
          return getAnnotatedUnion(union, names);
        } else if (c.isAnnotationPresent(Stringable.class)){ // Stringable
          Schema result = Schema.create(Schema.Type.STRING);
          result.addProp(CLASS_PROP, c.getName());
          return result;
        } else if (c.isEnum()) {                             // Enum
          List<String> symbols = new ArrayList<String>();
          Enum[] constants = (Enum[])c.getEnumConstants();
          for (int i = 0; i < constants.length; i++)
            symbols.add(constants[i].name());
          schema = Schema.createEnum(name, null /* doc */, space, symbols);
        } else if (GenericFixed.class.isAssignableFrom(c)) { // fixed
          int size = c.getAnnotation(FixedSize.class).value();
          schema = Schema.createFixed(name, null /* doc */, space, size);
        } else if (IndexedRecord.class.isAssignableFrom(c)) { // specific
          return super.createSchema(type, names);
        } else {                                             // record
          List<Schema.Field> fields = new ArrayList<Schema.Field>();
          boolean error = Throwable.class.isAssignableFrom(c);
          schema = Schema.createRecord(name, null /* doc */, space, error);
          names.put(c.getName(), schema);
          for (Field field : getFields(c))
            if ((field.getModifiers()&(Modifier.TRANSIENT|Modifier.STATIC))==0){
              Schema fieldSchema = createFieldSchema(field, names);
              JsonNode defaultValue = null;
              if (fieldSchema.getType() == Schema.Type.UNION) {
                Schema defaultType = fieldSchema.getTypes().get(0);
                if (defaultType.getType() == Schema.Type.NULL) {
                  defaultValue = NullNode.getInstance();
                }
              }
              fields.add(new Schema.Field(clean(field.getName()),
                  fieldSchema, null /* doc */, defaultValue));
            }
          if (error)                              // add Throwable message
            fields.add(new Schema.Field("detailMessage", THROWABLE_MESSAGE,
                                        null, null));
          schema.setFields(fields);
        }
        names.put(fullName, schema);
      }
      return schema;
    }
    return super.createSchema(type, names);
  }
  
  private static final Schema THROWABLE_MESSAGE =
      makeNullable(Schema.create(Schema.Type.STRING));
  

  @Override
  public Object getField(Object record, String name, int position) {
    if (record instanceof IndexedRecord)
      return super.getField(record, name, position);
    try {
      return getField(record.getClass(), name).get(record);
    } catch (IllegalAccessException e) {
      throw new AvroRuntimeException(e);
    }
  }
  
  private static final Map<Class,Map<String,Field>> FIELD_CACHE =
      new ConcurrentHashMap<Class,Map<String,Field>>();
  
  private static Field getField(Class c, String name) {
    Map<String,Field> fields = FIELD_CACHE.get(c);
    if (fields == null) {
      fields = new ConcurrentHashMap<String,Field>();
      FIELD_CACHE.put(c, fields);
    }
    Field f = fields.get(name);
    if (f == null) {
      f = findField(c, name);
      fields.put(name, f);
    }
    return f;
  }

  private static Field findField(Class original, String name) {
    Class c = original;
    do {
      try {
        Field f = c.getDeclaredField(dirty(name));
        f.setAccessible(true);
        return f;
      } catch (NoSuchFieldException e) {}
      c = c.getSuperclass();
    } while (c != null);
    throw new AvroRuntimeException("No field named "+name+" in: "+original);
  }
  
  private static String clean(String dirty) {
    return dirty.replace("$", "___");
  }
  
  private static String dirty(String clean) {
    return clean.replace("___", "$");
  }
  
  // Return of this class and its superclasses to serialize.
  // Not cached, since this is only used to create schemas, which are cached.
  private Collection<Field> getFields(Class recordClass) {
    Map<String,Field> fields = new LinkedHashMap<String,Field>();
    Class c = recordClass;
    do {
      if (c.getPackage() != null
          && c.getPackage().getName().startsWith("java."))
        break;                                    // skip java built-in classes
      for (Field field : c.getDeclaredFields())
        if ((field.getModifiers() & (Modifier.TRANSIENT|Modifier.STATIC)) == 0)
          if (fields.put(field.getName(), field) != null)
            throw new AvroTypeException(c+" contains two fields named: "+field);
      c = c.getSuperclass();
    } while (c != null);
    return fields.values();
  }
  
  @SuppressWarnings(value="unchecked")
  private void setElement(Schema schema, Type element) {
    if (!(element instanceof Class)) return;
    Class<?> c = (Class<?>)element;
    Union union = c.getAnnotation(Union.class);
    if (union != null)                          // element is annotated union
      schema.addProp(ELEMENT_PROP, c.getName());
  }
  
  // construct a schema from a union annotation
  private Schema getAnnotatedUnion(Union union, Map<String,Schema> names) {
    List<Schema> branches = new ArrayList<Schema>();
    for (Class branch : union.value())
      branches.add(createSchema(branch, names));
    return Schema.createUnion(branches);
  }
  
  @Override
  protected boolean isArray(Object datum) {
    if (datum == null) return false;
    return (datum instanceof Collection) || datum.getClass().isArray() ||
        (datum instanceof scala.collection.Iterable);
  }
  
  @Override
  protected boolean isMap(Object datum) {
    return (datum instanceof java.util.Map) || (datum instanceof scala.collection.Map);
  }

  @Override
  protected String getSchemaName(Object datum) {
    if (datum != null) {
      if(byte[].class.isAssignableFrom(datum.getClass())) {
        return Schema.Type.BYTES.getName();
      }
    }
    return super.getSchemaName(datum);
  }
}
