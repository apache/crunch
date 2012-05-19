package com.cloudera.crunch;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.io.At;
import com.cloudera.crunch.lib.SetTest;
import com.cloudera.crunch.test.FileHelper;
import com.cloudera.crunch.types.PTypeFamily;
import com.cloudera.crunch.types.avro.AvroTypeFamily;
import com.cloudera.crunch.types.writable.WritableTypeFamily;
import com.google.common.collect.Lists;

@RunWith(value = Parameterized.class)
public class PTableKeyValueTest implements Serializable {

	private static final long serialVersionUID = 4374227704751746689L;

	private transient PTypeFamily typeFamily;
	private transient MRPipeline pipeline;
	private transient String inputFile;

	@Before
	public void setUp() throws IOException {
		pipeline = new MRPipeline(PTableKeyValueTest.class);
		inputFile = FileHelper.createTempCopyOf("set1.txt");
	}

	@After
	public void tearDown() {
		pipeline.done();
	}

	public PTableKeyValueTest(PTypeFamily typeFamily) {
		this.typeFamily = typeFamily;
	}

	@Parameters
	public static Collection<Object[]> data() {
		Object[][] data = new Object[][] {
				{ WritableTypeFamily.getInstance() },
				{ AvroTypeFamily.getInstance() } };
		return Arrays.asList(data);
	}

	@Test
	public void testKeysAndValues() throws Exception {

		PCollection<String> collection = pipeline.read(At.textFile(inputFile,
				typeFamily.strings()));

		PTable<String, String> table = collection.parallelDo(
				new DoFn<String, Pair<String, String>>() {

					@Override
					public void process(String input,
							Emitter<Pair<String, String>> emitter) {
						emitter.emit(Pair.of(input.toUpperCase(), input));

					}
				}, typeFamily.tableOf(typeFamily.strings(),
						typeFamily.strings()));

		PCollection<String> keys = table.keys();
		PCollection<String> values = table.values();

		ArrayList<String> keyList = Lists.newArrayList(keys.materialize()
				.iterator());
		ArrayList<String> valueList = Lists.newArrayList(values.materialize()
				.iterator());

		Assert.assertEquals(keyList.size(), valueList.size());
		for (int i = 0; i < keyList.size(); i++) {
			Assert.assertEquals(keyList.get(i), valueList.get(i).toUpperCase());
		}
	}

}
