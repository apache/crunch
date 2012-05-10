/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.crunch.fn;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.Pair;

@SuppressWarnings("serial")
public class ExtractKeyFnTest {

	protected static final MapFn<String, Integer> mapFn = new MapFn<String, Integer>() {
		@Override
		public Integer map(String input) {
			return input.hashCode();
		}
	};

	protected static final ExtractKeyFn<Integer, String> one = new ExtractKeyFn<Integer, String>(
			mapFn);

	@Test
	public void test() {
		StoreLastEmitter<Pair<Integer, String>> emitter = StoreLastEmitter.create();
		one.process("boza", emitter);
		assertEquals(Pair.of("boza".hashCode(), "boza"), emitter.getLast());
	}
}
