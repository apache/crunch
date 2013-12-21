/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.crunch.types.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.crunch.io.FormatBundle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

public enum AvroMode implements ReaderWriterFactory {
  REFLECT (new ReflectDataFactory(), Avros.REFLECT_DATA_FACTORY_CLASS),
  SPECIFIC ("crunch.specificfactory"),
  GENERIC ("crunch.genericfactory");

  public static final String AVRO_MODE_PROPERTY = "crunch.avro.mode";
  public static final String AVRO_SHUFFLE_MODE_PROPERTY = "crunch.avro.shuffle.mode";

  public static AvroMode fromConfiguration(Configuration conf) {
    AvroMode mode = conf.getEnum(AVRO_MODE_PROPERTY, REFLECT);
    mode.setFromConfiguration(conf);
    return mode;
  }

  public static AvroMode fromShuffleConfiguration(Configuration conf) {
    AvroMode mode = conf.getEnum(AVRO_SHUFFLE_MODE_PROPERTY, REFLECT);
    mode.setFromConfiguration(conf);
    return mode;
  }

  public static AvroMode fromType(AvroType<?> type) {
    if (type.hasReflect()) {
      if (type.hasSpecific()) {
        Avros.checkCombiningSpecificAndReflectionSchemas();
      }
      return AvroMode.REFLECT;
    } else if (type.hasSpecific()) {
      return AvroMode.SPECIFIC;
    } else {
      return AvroMode.GENERIC;
    }
  }

  private static ClassLoader specificLoader = null;

  public static void setSpecificClassLoader(ClassLoader loader) {
    specificLoader = loader;
  }

  // the factory methods in this class may be overridden in ReaderWriterFactory
  ReaderWriterFactory factory;

  private final String propName;

  private AvroMode(ReaderWriterFactory factory, String propName) {
    this.factory = factory;
    this.propName = propName;
  }

  private AvroMode(String propName) {
    this.factory = null;
    this.propName = propName;
  }

  public GenericData getData() {
    if (factory != null) {
      return factory.getData();
    }

    switch(this) {
      case REFLECT:
        return ReflectData.AllowNull.get();
      case SPECIFIC:
        return SpecificData.get();
      default:
        return GenericData.get();
    }
  }

  public <T> DatumReader<T> getReader(Schema schema) {
    if (factory != null) {
      return factory.getReader(schema);
    }

    switch (this) {
      case REFLECT:
        return new ReflectDatumReader<T>(schema);
      case SPECIFIC:
        if (specificLoader != null) {
          return new SpecificDatumReader<T>(
              schema, schema, new SpecificData(specificLoader));
        } else {
          return new SpecificDatumReader<T>(schema);
        }
      default:
        return new GenericDatumReader<T>(schema);
    }
  }

  public <T> DatumWriter<T> getWriter(Schema schema) {
    if (factory != null) {
      return factory.getWriter(schema);
    }

    switch (this) {
      case REFLECT:
        return new ReflectDatumWriter<T>(schema);
      case SPECIFIC:
        return new SpecificDatumWriter<T>(schema);
      default:
        return new GenericDatumWriter<T>(schema);
    }
  }

  public void override(ReaderWriterFactory factory) {
    if (factory != this) {
      this.factory = factory;
    }
  }

  public void configureShuffle(Configuration conf) {
    conf.setEnum(AVRO_SHUFFLE_MODE_PROPERTY, this);
    configureFactory(conf);
  }

  public void configure(FormatBundle bundle) {
    bundle.set(AVRO_MODE_PROPERTY, this.toString());
    if (factory != null) {
      bundle.set(propName, factory.getClass().getName());
    }
  }

  public void configureFactory(Configuration conf) {
    if (factory != null) {
      conf.setClass(propName, factory.getClass(), ReaderWriterFactory.class);
    }
  }

  @SuppressWarnings("unchecked")
  void setFromConfiguration(Configuration conf) {
    // although the shuffle and input/output use different properties for mode,
    // this is shared - only one ReaderWriterFactory can be used.
    Class<?> factoryClass = conf.getClass(propName, this.getClass());
    if (factoryClass != this.getClass()) {
      this.factory = (ReaderWriterFactory)
          ReflectionUtils.newInstance(factoryClass, conf);
    }
  }
}
