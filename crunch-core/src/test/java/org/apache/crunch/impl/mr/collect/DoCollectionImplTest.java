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

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.impl.mr.plan.DoNode;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.writable.Writables;
import org.junit.Test;

public class DoCollectionImplTest {

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
    PCollectionImpl<String> parentCollection = new SizedPCollectionImpl("Sized collection", inputSize);

    DoCollectionImpl<String> doCollectionImpl = new DoCollectionImpl<String>("Scaled collection", parentCollection,
        new ScaledFunction(scaleFactor), Writables.strings());

    assertEquals(expectedScaledSize, doCollectionImpl.getSizeInternal());
  }

  static class ScaledFunction extends DoFn<String, String> {

    private float scaleFactor;

    public ScaledFunction(float scaleFactor) {
      this.scaleFactor = scaleFactor;
    }

    @Override
    public void process(String input, Emitter<String> emitter) {
      emitter.emit(input);
    }

    @Override
    public float scaleFactor() {
      return scaleFactor;
    }

  }

  static class SizedPCollectionImpl extends PCollectionImpl<String> {

    private long internalSize;

    public SizedPCollectionImpl(String name, long internalSize) {
      super(name);
      this.internalSize = internalSize;
    }

    @Override
    public PType getPType() {
      return null;
    }

    @Override
    public DoNode createDoNode() {
      return null;
    }

    @Override
    public List getParents() {
      return null;
    }

    @Override
    protected void acceptInternal(Visitor visitor) {
    }

    @Override
    protected long getSizeInternal() {
      return internalSize;
    }

    @Override
    public long getLastModifiedAt() {
      return -1;
    }
  }

}
