package com.cloudera.crunch.lib;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;

import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.lib.Cartesian;
import com.cloudera.crunch.types.PTypeFamily;
import com.cloudera.crunch.types.writable.Writables;
import com.cloudera.crunch.impl.mem.MemPipeline;
import com.google.common.collect.ImmutableList;

public class CartesianTest {
  
  @Test
  public void testCartesianCollection() {
    ImmutableList<ImmutableList<Integer>> testCases = ImmutableList.of(ImmutableList.of(1,2,3,4,5), ImmutableList.<Integer>of(1,2,3), ImmutableList.<Integer>of());
        
    for (int t1 = 0; t1 < testCases.size(); t1++) {
      ImmutableList<Integer> testCase1 = testCases.get(t1);
      for (int t2 = t1; t2 < testCases.size(); t2++) {
        ImmutableList<Integer> testCase2 = testCases.get(t2);
      
      	PCollection<Integer> X = MemPipeline.typedCollectionOf(Writables.ints(), testCase1);
      	PCollection<Integer> Y = MemPipeline.typedCollectionOf(Writables.ints(), testCase2);
      	
      	PCollection<Pair<Integer,Integer>> cross = Cartesian.cross(X, Y);
      	HashSet<Pair<Integer, Integer>> crossSet = new HashSet<Pair<Integer, Integer>>();
      	for (Iterator<Pair<Integer, Integer>> i = cross.materialize().iterator(); i.hasNext(); )
      	  crossSet.add(i.next());
      	  
      	assertEquals(crossSet.size(), testCase1.size() * testCase2.size());
      	System.out.println(crossSet.size());
      	
      	for (int i = 1; i <= 10; i++)
        	for (int j = 1; j <= 10; j++)
        	  assert(crossSet.contains(Pair.of(i, j)));
      }
    }
  }
	
}
