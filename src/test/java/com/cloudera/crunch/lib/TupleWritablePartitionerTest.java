package com.cloudera.crunch.lib;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.crunch.lib.join.JoinUtils.TupleWritablePartitioner;
import com.cloudera.crunch.types.writable.TupleWritable;

public class TupleWritablePartitionerTest {

	private TupleWritablePartitioner tupleWritableParitioner;
	
	@Before
	public void setUp(){
		tupleWritableParitioner = new TupleWritablePartitioner();
	}
	
	@Test
	public void testGetPartition() {
		IntWritable intWritable = new IntWritable(3);
		TupleWritable key = new TupleWritable(new Writable[]{intWritable});
		assertEquals(3, tupleWritableParitioner.getPartition(key, NullWritable.get(), 5));
		assertEquals(1, tupleWritableParitioner.getPartition(key, NullWritable.get(), 2));
	}
	
	@Test
	public void testGetPartition_NegativeHashValue(){
		IntWritable intWritable = new IntWritable(-3);
		// Sanity check, if this doesn't work then the premise of this test is wrong
		assertEquals(-3, intWritable.hashCode());
		
		TupleWritable key = new TupleWritable(new Writable[]{intWritable});
		assertEquals(3, tupleWritableParitioner.getPartition(key, NullWritable.get(), 5));
		assertEquals(1, tupleWritableParitioner.getPartition(key, NullWritable.get(), 2));
	}
	
	@Test
	public void testGetPartition_IntegerMinValue(){
		IntWritable intWritable = new IntWritable(Integer.MIN_VALUE);
		// Sanity check, if this doesn't work then the premise of this test is wrong
		assertEquals(Integer.MIN_VALUE, intWritable.hashCode());
		
		
		TupleWritable key = new TupleWritable(new Writable[]{intWritable});
		assertEquals(0, tupleWritableParitioner.getPartition(key, NullWritable.get(), Integer.MAX_VALUE));
	}

}
