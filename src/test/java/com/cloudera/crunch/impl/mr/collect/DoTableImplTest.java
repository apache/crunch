package com.cloudera.crunch.impl.mr.collect;

import static com.cloudera.crunch.types.writable.Writables.strings;
import static com.cloudera.crunch.types.writable.Writables.tableOf;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.Pair;

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
				parentCollection, new TableScaledFunction(scaleFactor), tableOf(strings(),
						strings()));

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
