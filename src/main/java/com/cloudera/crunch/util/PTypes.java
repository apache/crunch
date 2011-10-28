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
package com.cloudera.crunch.util;

import org.codehaus.jackson.map.ObjectMapper;

import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.impl.mr.run.CrunchRuntimeException;
import com.cloudera.crunch.type.PType;
import com.cloudera.crunch.type.PTypeFamily;

/**
 * Utility functions for creating common types of derived PTypes, e.g., for JSON data.
 *
 */
public class PTypes {

  public static <T> PType<T> jsonString(Class<T> clazz, PTypeFamily typeFamily) {
    return typeFamily.derived(clazz, new JSONInputManFn<T>(clazz), new JSONOutputMapFn<T>(), typeFamily.strings());
  }

  public static class JSONInputManFn<T> extends MapFn<String, T> {

    private final Class<T> clazz;
    private transient ObjectMapper mapper;
    
    public JSONInputManFn(Class<T> clazz) {
      this.clazz = clazz;
    }
    
    @Override
    public void initialize() {
      this.mapper = new ObjectMapper();
    }
    
    @Override
    public T map(String input) {
      try {
        return mapper.readValue(input, clazz);
      } catch (Exception e) {
        throw new CrunchRuntimeException(e);
      }
    } 
  }
  
  public static class JSONOutputMapFn<T> extends MapFn<T, String> {

    private transient ObjectMapper mapper;
    
    @Override
    public void initialize() {
      this.mapper = new ObjectMapper();
    }
    
    @Override
    public String map(T input) {
      try {
        return mapper.writeValueAsString(input);
      } catch (Exception e) {
        throw new CrunchRuntimeException(e);
      }
    }
  }
}
