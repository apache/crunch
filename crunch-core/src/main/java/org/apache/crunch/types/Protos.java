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
package org.apache.crunch.types;

import java.util.Iterator;
import java.util.List;

import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.base.Splitter;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;

/**
 * Utility functions for working with protocol buffers in Crunch.
 */
public class Protos {

  /**
   * Utility function for creating a default PB Messgae from a Class object that
   * works with both protoc 2.3.0 and 2.4.x.
   * @param clazz The class of the protocol buffer to create
   * @return An instance of a protocol buffer
   */
  public static <M extends Message> M getDefaultInstance(Class<M> clazz) {
    if (clazz.getConstructors().length > 0) {
      // Protobuf 2.3.0
      return ReflectionUtils.newInstance(clazz, null);
    } else {
      // Protobuf 2.4.x
      try {
        Message.Builder mb = (Message.Builder) clazz.getDeclaredMethod("newBuilder").invoke(null);
        return (M) mb.getDefaultInstanceForType();
      } catch (Exception e) {
        throw new CrunchRuntimeException(e);
      }  
    }
  }
  
  public static <M extends Message, K> MapFn<M, K> extractKey(String fieldName) {
    return new ExtractKeyFn<M, K>(fieldName);
  }

  public static <M extends Message> DoFn<String, M> lineParser(String sep, Class<M> msgClass) {
    return new TextToProtoFn<M>(sep, msgClass);
  }

  private static class ExtractKeyFn<M extends Message, K> extends MapFn<M, K> {

    private final String fieldName;

    private transient FieldDescriptor fd;

    public ExtractKeyFn(String fieldName) {
      this.fieldName = fieldName;
    }

    @Override
    public K map(M input) {
      if (input == null) {
        throw new IllegalArgumentException("Null inputs not supported by Protos.ExtractKeyFn");
      } else if (fd == null) {
        fd = input.getDescriptorForType().findFieldByName(fieldName);
        if (fd == null) {
          throw new IllegalStateException("Could not find field: " + fieldName + " in message: " + input);
        }
      }
      return (K) input.getField(fd);
    }

  }

  private static class TextToProtoFn<M extends Message> extends DoFn<String, M> {

    private final String sep;
    private final Class<M> msgClass;

    private transient M msgInstance;
    private transient List<FieldDescriptor> fields;
    private transient Splitter splitter;

    enum ParseErrors {
      TOTAL,
      NUMBER_FORMAT
    };

    public TextToProtoFn(String sep, Class<M> msgClass) {
      this.sep = sep;
      this.msgClass = msgClass;
    }

    @Override
    public void initialize() {
      this.msgInstance = getDefaultInstance(msgClass);
      this.fields = msgInstance.getDescriptorForType().getFields();
      this.splitter = Splitter.on(sep);
    }

    @Override
    public void process(String input, Emitter<M> emitter) {
      if (input != null && !input.isEmpty()) {
        Builder b = msgInstance.newBuilderForType();
        Iterator<String> iter = splitter.split(input).iterator();
        boolean parseError = false;
        for (FieldDescriptor fd : fields) {
          if (iter.hasNext()) {
            String value = iter.next();
            if (value != null && !value.isEmpty()) {
              Object parsedValue = null;
              try {
                switch (fd.getJavaType()) {
                case STRING:
                  parsedValue = value;
                  break;
                case INT:
                  parsedValue = Integer.valueOf(value);
                  break;
                case LONG:
                  parsedValue = Long.valueOf(value);
                  break;
                case FLOAT:
                  parsedValue = Float.valueOf(value);
                  break;
                case DOUBLE:
                  parsedValue = Double.valueOf(value);
                  break;
                case BOOLEAN:
                  parsedValue = Boolean.valueOf(value);
                  break;
                case ENUM:
                  parsedValue = fd.getEnumType().findValueByName(value);
                  break;
                }
                b.setField(fd, parsedValue);
              } catch (NumberFormatException nfe) {
                increment(ParseErrors.NUMBER_FORMAT);
                parseError = true;
                break;
              }
            }
          }
        }

        if (parseError) {
          increment(ParseErrors.TOTAL);
        } else {
          emitter.emit((M) b.build());
        }
      }
    }
  }

}
