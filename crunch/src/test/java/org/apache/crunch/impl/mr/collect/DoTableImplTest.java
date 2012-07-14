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
package org.apache.crunch.impl.mr.collect;

import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.junit.Test;

public class DoTableImplTest {

  @Test
  public void testGetSizeInternal_NoScaleFactor() {
    runScaleTest(100L, 1.0f, 100L);
  }

  @Test
  public void testGetSizeInternal_ScaleFactorBelowZero() {
    runScaleTest(100L, 0.5f, 50L);
  }

  @Test
  public void testGetSizeInternal_ScaleFactorAboveZero() {
    runScaleTest(100L, 1.5f, 150L);
  }

  private void runScaleTest(long inputSize, float scaleFactor, long expectedScaledSize) {

    @SuppressWarnings("unchecked")
    PCollectionImpl<String> parentCollection = (PCollectionImpl<String>) mock(PCollectionImpl.class);

    when(parentCollection.getSize()).thenReturn(inputSize);

    DoTableImpl<String, String> doTableImpl = new DoTableImpl<String, String>("Scalled table collection",
        parentCollection, new TableScaledFunction(scaleFactor), tableOf(strings(), strings()));

    assertEquals(expectedScaledSize, doTableImpl.getSizeInternal());

    verify(parentCollection).getSize();

    verifyNoMoreInteractions(parentCollection);
  }

  static class TableScaledFunction extends DoFn<String, Pair<String, String>> {

    private float scaleFactor;

    public TableScaledFunction(float scaleFactor) {
      this.scaleFactor = scaleFactor;
    }

    @Override
    public float scaleFactor() {
      return scaleFactor;
    }

    @Override
    public void process(String input, Emitter<Pair<String, String>> emitter) {
      emitter.emit(Pair.of(input, input));

    }
  }
}
