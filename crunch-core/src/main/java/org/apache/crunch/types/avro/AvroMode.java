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

import com.google.common.collect.Maps;
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

import java.util.Map;

/**
 * AvroMode is an immutable object used for configuring the reading and writing of Avro types.
 * The mode will not be used or honored unless it has been appropriately configured using one of the supported
 * methods.  Certain sources might also support specifying a specific mode to use.
 */
public class AvroMode implements ReaderWriterFactory {

  /**
   * Internal enum which represents the various Avro data types.
   */
  public static enum ModeType {
    SPECIFIC, REFLECT, GENERIC
  }

  /**
   * Default mode to use for reading and writing {@link ReflectData Reflect} types.
   */
  public static final AvroMode REFLECT = new AvroMode(ModeType.REFLECT, Avros.REFLECT_DATA_FACTORY_CLASS);

  /**
   * Default mode to use for reading and writing {@link SpecificData Specific} types.
   */
  public static final AvroMode SPECIFIC = new AvroMode(ModeType.SPECIFIC, "crunch.specificfactory");
  /**
   * Default mode to use for reading and writing {@link GenericData Generic} types.
   */
  public static final AvroMode GENERIC = new AvroMode(ModeType.GENERIC, "crunch.genericfactory");

  public static final String AVRO_MODE_PROPERTY = "crunch.avro.mode";
  public static final String AVRO_SHUFFLE_MODE_PROPERTY = "crunch.avro.shuffle.mode";

  /**
   * Creates an AvroMode based on the {@link #AVRO_MODE_PROPERTY} property in the {@code conf}.
   * @param conf The configuration holding the properties for mode to be created.
   * @return an AvroMode based on the {@link #AVRO_MODE_PROPERTY} property in the {@code conf}.
   */
  public static AvroMode fromConfiguration(Configuration conf) {
    AvroMode mode = getMode(conf.getEnum(AVRO_MODE_PROPERTY, ModeType.REFLECT));
    return mode.withFactoryFromConfiguration(conf);
  }

  /**
   * Creates an AvroMode based on the {@link #AVRO_SHUFFLE_MODE_PROPERTY} property in the {@code conf}.
   * @param conf The configuration holding the properties for mode to be created.
   * @return an AvroMode based on the {@link #AVRO_SHUFFLE_MODE_PROPERTY} property in the {@code conf}.
   */
  public static AvroMode fromShuffleConfiguration(Configuration conf) {
    AvroMode mode = getMode(conf.getEnum(AVRO_SHUFFLE_MODE_PROPERTY, ModeType.REFLECT));
    return mode.withFactoryFromConfiguration(conf);
  }

  /**
   * Creates an {@link AvroMode} based upon the specified {@code type}.
   * @param type the Avro type which indicates a specific mode.
   * @return an {@link AvroMode} based upon the specified {@code type}.
   */
  public static AvroMode fromType(AvroType<?> type) {
    if (type.hasReflect()) {
      if (type.hasSpecific()) {
        Avros.checkCombiningSpecificAndReflectionSchemas();
      }
      return REFLECT;
    } else if (type.hasSpecific()) {
      return SPECIFIC;
    } else {
      return GENERIC;
    }
  }

  private static AvroMode getMode(ModeType modeType){
    switch(modeType){
        case SPECIFIC:
          return SPECIFIC;
        case GENERIC:
          return GENERIC;
        case REFLECT:
        default:
          return REFLECT;
    }
  }

  private static ClassLoader specificLoader = null;

  /**
   * Set the {@code ClassLoader} that will be used for loading Avro {@code org.apache.avro.specific.SpecificRecord}
   * and reflection implementation classes. It is typically not necessary to call this method -- it should only be used
   * if a specific class loader is needed in order to load the specific datum classes.
   *
   * @param loader the {@code ClassLoader} to be used for loading specific datum classes
   */
  public static void setSpecificClassLoader(ClassLoader loader) {
    specificLoader = loader;
  }

  /**
   * Get the configured {@code ClassLoader} to be used for loading Avro {@code org.apache.specific.SpecificRecord}
   * and reflection implementation classes. The return value may be null.
   *
   * @return the configured {@code ClassLoader} for loading specific or reflection datum classes, may be null
   */
  public static ClassLoader getSpecificClassLoader() {
    return specificLoader;
  }

  /**
   * Internal method for setting the specific class loader if none is already set. If no specific class loader is set,
   * the given class loader will be set as the specific class loader. If a specific class loader is already set, this
   * will be a no-op.
   *
   * @param loader the {@code ClassLoader} to be registered as the specific class loader if no specific class loader
   *               is already set
   */
  static void registerSpecificClassLoaderInternal(ClassLoader loader) {
    if (specificLoader == null) {
      setSpecificClassLoader(loader);
    }
  }

  /**
   * the factory methods in this class may be overridden in ReaderWriterFactory
   */
  private final ReaderWriterFactory factory;

  /**
   * The property name used setting property into {@link Configuration}.
   */
  private final String propName;

  /**
   * The mode type representing the Avro data form.
   */
  private final ModeType modeType;

  private AvroMode(ModeType modeType, ReaderWriterFactory factory, String propName) {
    this.factory = factory;
    this.propName = propName;
    this.modeType = modeType;
  }

  private AvroMode(ModeType modeType, String propName) {
    this(modeType, null, propName);
  }

  /**
   * Returns a {@link GenericData} instance based on the mode type.
   * @return a {@link GenericData} instance based on the mode type.
   */
  public GenericData getData() {
    if (factory != null) {
      return factory.getData();
    }

    switch(this.modeType) {
      case REFLECT:
        return ReflectData.AllowNull.get();
      case SPECIFIC:
        return SpecificData.get();
      default:
        return GenericData.get();
    }
  }

  /**
   * Creates a {@code DatumReader} based on the {@code schema}.
   * @param schema the schema to be read
   * @param <T> the record type created by the reader.
   * @return a {@code DatumReader} based on the {@code schema}.
   */
  public <T> DatumReader<T> getReader(Schema schema) {
    if (factory != null) {
      return factory.getReader(schema);
    }

    switch (this.modeType) {
      case REFLECT:
        if (specificLoader != null) {
          return new ReflectDatumReader<T>(schema, schema, new ReflectData(specificLoader));
        } else {
          return new ReflectDatumReader<T>(schema);
        }
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

  /**
   * Creates a {@code DatumWriter} based on the {@code schema}.
   * @param schema the schema to be read
   * @param <T> the record type created by the writer.
   * @return a {@code DatumWriter} based on the {@code schema}.
   */
  public <T> DatumWriter<T> getWriter(Schema schema) {
    if (factory != null) {
      return factory.getWriter(schema);
    }

    switch (this.modeType) {
      case REFLECT:
        return new ReflectDatumWriter<T>(schema);
      case SPECIFIC:
        return new SpecificDatumWriter<T>(schema);
      default:
        return new GenericDatumWriter<T>(schema);
    }
  }

  /**
   * Creates a new {@code AvroMode} instance which will utilize the {@code factory} instance
   * for creating Avro readers and writers.
   *
   * @param factory factory implementation for the mode to use
   * @return a new {@code AvroMode} instance which will utilize the {@code factory} instance
   * for creating Avro readers and writers.
   * @deprecated use {@link #withFactory(ReaderWriterFactory)} instead.
   */
  @Deprecated
  public AvroMode override(ReaderWriterFactory factory) {
    return withFactory(factory);
  }

  /**
   * Creates a new {@code AvroMode} instance which will utilize the {@code factory} instance
   * for creating Avro readers and writers.  If {@code null} the default factory for the mode
   * will be used.
   *
   * @param factory factory implementation for the mode to use
   * @return a new {@code AvroMode} instance which will utilize the {@code factory} instance
   * for creating Avro readers and writers.
   */
  public AvroMode withFactory(ReaderWriterFactory factory){
    if (factory != this) {
      return withReaderWriterFactory(factory);
    } else {
      return this;
    }
  }

  /**
   * Populates the {@code conf} with mode specific settings for use during the shuffle phase.
   * @param conf the configuration to populate.
   */
  public void configureShuffle(Configuration conf) {
    conf.setEnum(AVRO_SHUFFLE_MODE_PROPERTY, this.modeType);
    configure(conf);
  }

  /**
   * Populates the {@code bundle} with mode specific settings for the specific {@link FormatBundle}.
   * @param bundle the bundle to populate.
   */
  public void configure(FormatBundle bundle) {
    bundle.set(AVRO_MODE_PROPERTY, this.modeType.toString());
    if (factory != null) {
      bundle.set(propName, factory.getClass().getName());
    }
  }

  /**
   * Populates the {@code conf} with mode specific settings.
   * @param conf the configuration to populate.
   */
  public void configure(Configuration conf) {
    conf.set(AVRO_MODE_PROPERTY, this.modeType.toString());
    if (factory != null) {
      conf.setClass(propName, factory.getClass(), ReaderWriterFactory.class);
    }
  }

  /**
   * Returns the entries that a {@code Configuration} instance needs to enable
   * this AvroMode as a serializable map of key-value pairs.
   */
  public Map<String, String> getModeProperties() {
    Map<String, String> props = Maps.newHashMap();
    props.put(AVRO_MODE_PROPERTY, this.modeType.toString());
    if (factory != null) {
      props.put(propName, factory.getClass().getCanonicalName());
    }
    return props;
  }

  /**
   * Populates the {@code conf} with mode specific settings.
   * @param conf the configuration to populate.
   * @deprecated use {@link #configure(org.apache.hadoop.conf.Configuration)}
   */
  @Deprecated
  public void configureFactory(Configuration conf) {
    configure(conf);
  }

  /**
   * Creates a new {@code AvroMode} instance which will utilize the {@code factory} instance
   * for creating Avro readers and writers.  If {@code null} the default factory for the mode
   * will be used.
   *
   * @param readerWriterFactory factory implementation for the mode to use
   * @return a new {@code AvroMode} instance which will utilize the {@code factory} instance
   * for creating Avro readers and writers.
   */
  private AvroMode withReaderWriterFactory(ReaderWriterFactory readerWriterFactory) {
    return new AvroMode(modeType, readerWriterFactory, propName);
  }

  /**
   * Returns the factory that will be used for the mode.
   *
   * @return the factory that will be used for the mode.
   */
  public ReaderWriterFactory getFactory() {
    return factory != null ? factory : this;
  }

  @Override
  public boolean equals(Object o) {
    if(o == null){
      return false;
    }

    if(this == o){
      return true;
    }

    if(!(o instanceof AvroMode)){
      return false;
    }

    AvroMode that = (AvroMode) o;

    if(!this.modeType.equals(that.modeType)){
      return false;
    }
    if(!this.propName.equals(that.propName)){
      return false;
    }

    if(this.factory != null){
      if(that.factory == null){
        return false;
      }else {
        return this.factory.equals(that.factory);
      }
    }else{
      return that.factory == null;
    }
  }

  @Override
  public int hashCode() {
    int hash = propName.hashCode();
    hash = 31*hash + modeType.hashCode();
    if(factory != null){
      hash = 31*hash+factory.hashCode();
    }

    return hash;
  }

  @SuppressWarnings("unchecked")
  public AvroMode withFactoryFromConfiguration(Configuration conf) {
    // although the shuffle and input/output use different properties for mode,
    // this is shared - only one ReaderWriterFactory can be used.
    Class<?> factoryClass = conf.getClass(propName, this.getClass());
    if (factoryClass != this.getClass()) {
      return withReaderWriterFactory((ReaderWriterFactory)
                                         ReflectionUtils.newInstance(factoryClass, conf));
    } else {
      return this;
    }
  }
}
