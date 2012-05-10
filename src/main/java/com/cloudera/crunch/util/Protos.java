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
package com.cloudera.crunch.util;

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.util.ReflectionUtils;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.MapFn;
import com.google.common.base.Splitter;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message.Builder;

/**
 * Utility functions for working with protocol buffers in Crunch.
 */
public class Protos {

  public static <M extends Message, K> MapFn<M, K> extractKey(String fieldName) {
    return new ExtractKeyFn<M, K>(fieldName);
  }
  
  public static <M extends Message> DoFn<String, M> lineParser(String sep, Class<M> msgClass) {
    return new TextToProtoFn<M>(sep, msgClass);  
  }
  
  public static class ExtractKeyFn<M extends Message, K> extends MapFn<M, K> {
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
          throw new IllegalStateException(
              "Could not find field: " + fieldName + " in message: " + input);
        }
      }
      return (K) input.getField(fd);
    }
    
  }
  
  public static class TextToProtoFn<M extends Message> extends DoFn<String, M> {
    private final String sep;
    private final Class<M> msgClass;
    
    private transient M msgInstance;
    private transient List<FieldDescriptor> fields;
    private transient Splitter splitter;
    
    enum ParseErrors { TOTAL, NUMBER_FORMAT };
    
    public TextToProtoFn(String sep, Class<M> msgClass) {
      this.sep = sep;
      this.msgClass = msgClass;
    }
    
    @Override
    public void initialize() {
      this.msgInstance = ReflectionUtils.newInstance(msgClass, getConfiguration());
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
