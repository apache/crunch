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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.crunch.io.FormatBundle;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class AvroModeTest {

  @Test
  public void customWithFactory(){
    ReaderWriterFactory fakeFactory = new FakeReaderWriterFactory();
    AvroMode mode = AvroMode.SPECIFIC.withFactory(fakeFactory);
    assertThat(mode.getFactory(), is(fakeFactory));
    //assert that the original is unchanged
    assertThat(mode, is(not(AvroMode.SPECIFIC)));
    assertThat(AvroMode.SPECIFIC.getFactory(), is((ReaderWriterFactory) AvroMode.SPECIFIC));
  }

  @Test
  public void sameWithFactory(){
    AvroMode mode = AvroMode.SPECIFIC.withFactory(AvroMode.SPECIFIC);
    assertThat(mode.getFactory(), is( (ReaderWriterFactory) AvroMode.SPECIFIC));
  }

  @Test
  public void getDataSpecific(){
    assertThat(AvroMode.SPECIFIC.getData(), is(instanceOf(SpecificData.class)));
  }

  @Test
  public void getDataGeneric(){
    assertThat(AvroMode.GENERIC.getData(), is(instanceOf(GenericData.class)));
  }

  @Test
  public void getDataReflect(){
    assertThat(AvroMode.REFLECT.getData(), is(instanceOf(ReflectData.class)));
  }

  @Test
  public void configureAndRetrieveSpecific(){
    Configuration conf = new Configuration();
    AvroMode.SPECIFIC.configure(conf);
    AvroMode returnedMode = AvroMode.fromConfiguration(conf);
    assertThat(returnedMode, is(AvroMode.SPECIFIC));
  }

  @Test
  public void configureAndRetrieveGeneric(){
    Configuration conf = new Configuration();
    AvroMode.GENERIC.configure(conf);
    AvroMode returnedMode = AvroMode.fromConfiguration(conf);
    assertThat(returnedMode, is(AvroMode.GENERIC));
  }

  @Test
  public void configureShuffleAndRetrieveSpecific(){
    Configuration conf = new Configuration();
    AvroMode.SPECIFIC.configureShuffle(conf);
    AvroMode returnedMode = AvroMode.fromShuffleConfiguration(conf);
    assertThat(returnedMode, is(AvroMode.SPECIFIC));
  }

  @Test
  public void configureShuffleAndRetrieveGeneric(){
    Configuration conf = new Configuration();
    AvroMode.GENERIC.configureShuffle(conf);
    AvroMode returnedMode = AvroMode.fromShuffleConfiguration(conf);
    assertThat(returnedMode, is(AvroMode.GENERIC));
  }

  @Test
  public void configureBundleSpecific(){
    FormatBundle bundle = FormatBundle.forInput(AvroInputFormat.class);
    Configuration config = new Configuration();
    AvroMode.SPECIFIC.configure(bundle);
    bundle.configure(config);
    AvroMode returnedMode = AvroMode.fromConfiguration(config);
    assertThat(returnedMode.getData(), is(instanceOf(SpecificData.class)));
  }

  @Test
  public void configureBundleGeneric(){
    FormatBundle bundle = FormatBundle.forInput(AvroInputFormat.class);
    Configuration config = new Configuration();
    AvroMode.GENERIC.configure(bundle);
    bundle.configure(config);
    AvroMode returnedMode = AvroMode.fromConfiguration(config);
    assertThat(returnedMode.getData(), is(instanceOf(GenericData.class)));
  }

  @Test
  public void configureBundleReflect(){
    FormatBundle bundle = FormatBundle.forInput(AvroInputFormat.class);
    Configuration config = new Configuration();
    AvroMode.REFLECT.configure(bundle);
    bundle.configure(config);
    AvroMode returnedMode = AvroMode.fromConfiguration(config);
    assertThat(returnedMode.getData(), is(instanceOf(ReflectData.class)));
  }

  @Test
  public void configureBundleCustomWithFactory(){
    ReaderWriterFactory fakeFactory = new FakeReaderWriterFactory();
    AvroMode mode = AvroMode.SPECIFIC.withFactory(fakeFactory);
    FormatBundle bundle = FormatBundle.forInput(AvroInputFormat.class);
    Configuration config = new Configuration();
    mode.configure(bundle);
    bundle.configure(config);
    AvroMode returnedMode = AvroMode.fromConfiguration(config);
    assertThat(returnedMode.getFactory(), is(instanceOf(FakeReaderWriterFactory.class)));
  }

  @Test
  public void configureCustomWithFactory(){
    ReaderWriterFactory fakeFactory = new FakeReaderWriterFactory();
    AvroMode mode = AvroMode.SPECIFIC.withFactory(fakeFactory);
    Configuration config = new Configuration();
    mode.configure(config);
    AvroMode returnedMode = AvroMode.fromConfiguration(config);
    assertThat(returnedMode.getFactory(), is(instanceOf(FakeReaderWriterFactory.class)));
  }

  @Test
  public void testRegisterClassLoader() {
    // First make sure things are in the default situation
    AvroMode.setSpecificClassLoader(null);

    ClassLoader classLoaderA = mock(ClassLoader.class);
    ClassLoader classLoaderB = mock(ClassLoader.class);

    // Basic sanity check to ensure that the class loader was really nulled out
    assertNull(AvroMode.getSpecificClassLoader());

    // Do an internal registration of a class loader. Because there is currently no internal class loader set,
    // this should set the internal specific class loader
    AvroMode.registerSpecificClassLoaderInternal(classLoaderA);

    assertEquals(classLoaderA, AvroMode.getSpecificClassLoader());

    // Now we do an internal register of another class loader. Because there already is an internal specific class
    // loader set, this should have no impact (as opposed to calling setSpecificClassLoader)
    AvroMode.registerSpecificClassLoaderInternal(classLoaderB);

    assertEquals(classLoaderA, AvroMode.getSpecificClassLoader());
  }

  private static class FakeReaderWriterFactory implements ReaderWriterFactory{

    @Override
    public GenericData getData() {
      return null;
    }

    @Override
    public <D> DatumReader<D> getReader(Schema schema) {
      return null;
    }

    @Override
    public <D> DatumWriter<D> getWriter(Schema schema) {
      return null;
    }
  }

}
