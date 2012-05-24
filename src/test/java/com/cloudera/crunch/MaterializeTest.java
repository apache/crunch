package com.cloudera.crunch;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

import com.cloudera.crunch.impl.mem.MemPipeline;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.test.FileHelper;
import com.cloudera.crunch.types.PTypeFamily;
import com.cloudera.crunch.types.avro.AvroTypeFamily;
import com.cloudera.crunch.types.writable.WritableTypeFamily;
import com.google.common.collect.Lists;

public class MaterializeTest {

	/** Filter that rejects everything. */
	@SuppressWarnings("serial")
	private static class FalseFilterFn extends FilterFn<String> {

		@Override
		public boolean accept(final String input) {
			return false;
		}
	}

	@Test
	public void testMaterializeInput_Writables() throws IOException {
		runMaterializeInput(new MRPipeline(MaterializeTest.class), WritableTypeFamily.getInstance());
	}

	@Test
	public void testMaterializeInput_Avro() throws IOException {
		runMaterializeInput(new MRPipeline(MaterializeTest.class), AvroTypeFamily.getInstance());
	}

	@Test
	public void testMaterializeInput_InMemoryWritables() throws IOException {
		runMaterializeInput(MemPipeline.getInstance(), WritableTypeFamily.getInstance());
	}

	@Test
	public void testMaterializeInput_InMemoryAvro() throws IOException {
		runMaterializeInput(MemPipeline.getInstance(), AvroTypeFamily.getInstance());
	}

	@Test
	public void testMaterializeEmptyIntermediate_Writables() throws IOException {
		runMaterializeEmptyIntermediate(new MRPipeline(MaterializeTest.class),
				WritableTypeFamily.getInstance());
	}

	@Test
	public void testMaterializeEmptyIntermediate_Avro() throws IOException {
		runMaterializeEmptyIntermediate(new MRPipeline(MaterializeTest.class),
				AvroTypeFamily.getInstance());
	}

	@Test
	public void testMaterializeEmptyIntermediate_InMemoryWritables() throws IOException {
		runMaterializeEmptyIntermediate(MemPipeline.getInstance(), WritableTypeFamily.getInstance());
	}

	@Test
	public void testMaterializeEmptyIntermediate_InMemoryAvro() throws IOException {
		runMaterializeEmptyIntermediate(MemPipeline.getInstance(), AvroTypeFamily.getInstance());
	}

	public void runMaterializeInput(Pipeline pipeline, PTypeFamily typeFamily) throws IOException {
		List<String> expectedContent = Lists.newArrayList("b", "c", "a", "e");
		String inputPath = FileHelper.createTempCopyOf("set1.txt");

		PCollection<String> lines = pipeline.readTextFile(inputPath);
		assertEquals(expectedContent, Lists.newArrayList(lines.materialize()));
		pipeline.done();
	}

	public void runMaterializeEmptyIntermediate(Pipeline pipeline, PTypeFamily typeFamily)
			throws IOException {
		String inputPath = FileHelper.createTempCopyOf("set1.txt");
		PCollection<String> empty = pipeline.readTextFile(inputPath).filter(new FalseFilterFn());

		assertTrue(Lists.newArrayList(empty.materialize()).isEmpty());
		pipeline.done();
	}
}
