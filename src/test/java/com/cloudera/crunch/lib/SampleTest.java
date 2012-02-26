package com.cloudera.crunch.lib;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

import com.cloudera.crunch.impl.mem.MemPipeline;
import com.google.common.collect.ImmutableList;

public class SampleTest {
  @Test
  public void testSampler() {
	Iterable<Integer> sample = MemPipeline.collectionOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	    .sample(0.2, 123998).materialize();
	List<Integer> sampleValues = ImmutableList.copyOf(sample);
	assertEquals(ImmutableList.of(6, 7), sampleValues);
  }
}
