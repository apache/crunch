package com.cloudera.crunch.io;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.impl.mem.MemPipeline;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.type.writable.WritableType;
import com.cloudera.crunch.type.writable.Writables;
import com.google.common.collect.Lists;

public class InputCollectionGetSizeTest {

	private static final String TEST_RESOURCES = "src/test/resources/";
	private static final WritableType<String, Text> STRINGS = Writables
			.strings();
	// private PCollection<String> fixtures;
	// private Pipeline mrPipeline;
	private static File outputTempFile;

	static class PCollectionEmptier<T> extends DoFn<T, T> {

		private static final long serialVersionUID = -6763802293897483873L;

		@SuppressWarnings("unused")
		@Override
		public void process(T input, Emitter<T> emitter) {
			// Drop input
		}

	}

	@Before
	public void setUp() throws IOException {
		// mrPipeline = new MRPipeline(this.getClass());
		outputTempFile = File.createTempFile("crunchTest", "");
	}

	@After
	public void after() throws IOException {
		outputTempFile.delete();
	}

	@Test
	public void getSizeOfEmptyTextFileSourceMR() throws IOException {
		getSizeOfEmptyTextFileSource(new MRPipeline(this.getClass()));
	}

	@Test
	public void getSizeOfEmptyTextFileSourceMem() {
		getSizeOfEmptyTextFileSource(MemPipeline.getInstance());
	}

	private void getSizeOfEmptyTextFileSource(Pipeline pipeline) {
		PCollection<String> readTextFile = pipeline.readTextFile(TEST_RESOURCES
				+ "emptyTextFile.txt");
		long size = readTextFile.getSize();
		assertEquals(0, size);
	}

	@Test
	public void getSizeOfEmptySeqFileMR() throws IOException {
		getSizeOfEmptySeqFile(new MRPipeline(this.getClass()),
				outputTempFile.getAbsolutePath());
	}

	@Test
	public void getSizeOfEmptySeqFileMem() {
		getSizeOfEmptySeqFile(MemPipeline.getInstance(),
				outputTempFile.getAbsolutePath());
	}

	private void getSizeOfEmptySeqFile(Pipeline pipeline, String path) {

		PCollection<String> data = pipeline.readTextFile(TEST_RESOURCES
				+ "nonEmptyTextFile.txt");

		PCollection<String> emptyPCollection = data.parallelDo(
				new PCollectionEmptier<String>(), STRINGS);

		emptyPCollection.write(To.sequenceFile(path));
		pipeline.run();
		PCollection<String> read = pipeline.read(From.sequenceFile(path,
				STRINGS));

		long size = read.getSize();
		assertEquals(0, size);
	}

	@Test
	public void materializeEmptySeqFileMR() throws IOException {

		PCollection<String> read = writeNonEmptyAndReadEmptyFile(new MRPipeline(
				this.getClass()));

		long size = Lists.newArrayList(read.materialize()).size();
		
		assertEquals(0, size);
	}

	@Test
	public void materializeEmptySeqFileMem() {

		PCollection<String> read = writeNonEmptyAndReadEmptyFile(MemPipeline
				.getInstance());

		long size = Lists.newArrayList(read.materialize()).size();
		
		assertEquals(0, size);
	}

	private PCollection<String> writeNonEmptyAndReadEmptyFile(Pipeline pipeline) {

		PCollection<String> data = pipeline.readTextFile(TEST_RESOURCES
				+ "nonEmptyTextFile.txt");

		String path = outputTempFile.getAbsolutePath();

		PCollection<String> emptyPCollection = data.parallelDo(
				new PCollectionEmptier<String>(), STRINGS);

		emptyPCollection.write(To.sequenceFile(path));
		pipeline.run();
		
		PCollection<String> read = pipeline.read(From.sequenceFile(path,
				STRINGS));
		
		return read;
	}

	@Test(expected = IllegalStateException.class)
	public void expectExceptionForGettingSizeOfNonExistingFileMR()
			throws IOException {
		PCollection<String> readTextFile = new MRPipeline(
				this.getClass())
				.readTextFile(TEST_RESOURCES + "non_existing.file");
		readTextFile.getSize();
	}

	// @Test(expected = IllegalStateException.class)
	public void expectExceptionForGettingSizeOfNonExistingFileMem() {
		PCollection<String> readTextFile = MemPipeline.getInstance()
				.readTextFile(TEST_RESOURCES + "non_existing.file");
		readTextFile.getSize();
	}
}
