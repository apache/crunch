package com.cloudera.crunch.io.avro;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.junit.Test;

import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.test.FileHelper;
import com.cloudera.crunch.types.avro.Avros;
import com.google.common.collect.Lists;

public class AvroReflectTest implements Serializable {

	static class StringWrapper {
		private String value;

		public StringWrapper() {
			this(null);
		}

		public StringWrapper(String value) {
			this.value = value;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}

		@Override
		public String toString() {
			return String.format("<StringWrapper(%s)>", value);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((value == null) ? 0 : value.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			StringWrapper other = (StringWrapper) obj;
			if (value == null) {
				if (other.value != null)
					return false;
			} else if (!value.equals(other.value))
				return false;
			return true;
		}

	}

	@Test
	public void testReflection() throws IOException {
		Pipeline pipeline = new MRPipeline(AvroReflectTest.class);
		PCollection<StringWrapper> stringWrapperCollection = pipeline
				.readTextFile(FileHelper.createTempCopyOf("set1.txt"))
				.parallelDo(new MapFn<String, StringWrapper>() {

					@Override
					public StringWrapper map(String input) {
						StringWrapper stringWrapper = new StringWrapper();
						stringWrapper.setValue(input);
						return stringWrapper;
					}
				}, Avros.reflects(StringWrapper.class));

		List<StringWrapper> stringWrappers = Lists
				.newArrayList(stringWrapperCollection.materialize());

		pipeline.done();

		assertEquals(Lists.newArrayList(new StringWrapper("b"),
				new StringWrapper("c"), new StringWrapper("a"),
				new StringWrapper("e")), stringWrappers);

	}

}
