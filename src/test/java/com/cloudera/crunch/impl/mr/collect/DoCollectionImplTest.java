package com.cloudera.crunch.impl.mr.collect;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.fn.IdentityFn;
import com.cloudera.crunch.impl.mr.plan.DoNode;
import com.cloudera.crunch.types.PType;
import com.cloudera.crunch.types.writable.Writables;

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
  
  private void runScaleTest(long inputSize, float scaleFactor, long expectedScaledSize){
    PCollectionImpl<String> parentCollection = new SizedPCollectionImpl(
        "Sized collection", inputSize);
    
    DoCollectionImpl<String> doCollectionImpl = new DoCollectionImpl<String>(
        "Scaled collection", parentCollection, new ScaledFunction(scaleFactor),
        Writables.strings());

    assertEquals(expectedScaledSize, doCollectionImpl.getSizeInternal()); 
  }
  

  static class ScaledFunction extends DoFn<String, String>{
    
    private float scaleFactor;

    public ScaledFunction(float scaleFactor){
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

  }

}
