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
package com.cloudera.crunch.io;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Functions for configuring the inputs/outputs of MapReduce jobs.
 * 
 */
public class SourceTargetHelper {

	private static final Log LOG = LogFactory.getLog(SourceTargetHelper.class);

	public static long getPathSize(Configuration conf, Path path) throws IOException {
		return getPathSize(FileSystem.get(conf), path);
	}

	public static long getPathSize(FileSystem fs, Path path) throws IOException {

		if (!fs.exists(path)) {
			return -1L;
		}

		FileStatus[] stati = null;
		try {
			stati = fs.listStatus(path);
			if (stati == null) {
				throw new FileNotFoundException(path + " doesn't exist");
			}
		} catch (FileNotFoundException e) {
			LOG.warn("Returning 0 for getPathSize on non-existant path '" + path + "'");
			return 0L;
		}

		if (stati.length == 0) {
			return 0L;
		}
		long size = 0;
		for (FileStatus status : stati) {
			size += status.getLen();
		}
		return size;
	}
}
