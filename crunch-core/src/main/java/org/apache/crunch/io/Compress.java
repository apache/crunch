/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.crunch.io;

import org.apache.avro.file.DataFileConstants;
import org.apache.avro.mapred.AvroJob;
import org.apache.crunch.Target;
import org.apache.crunch.io.parquet.AvroParquetFileSourceTarget;
import org.apache.crunch.io.parquet.AvroParquetFileTarget;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;

/**
 * Helper functions for compressing output data.
 */
public class Compress {

  /**
   * Configure the given output target to be compressed using the given codec.
   */
  public static <T extends Target> T compress(T target, Class<? extends CompressionCodec> codecClass) {
    return (T) target.outputConf("mapred.output.compress", "true")
        .outputConf("mapred.output.compression.codec", codecClass.getCanonicalName());
  }

  /**
   * Configure the given output target to be compressed using Gzip.
   */
  public static <T extends Target> T gzip(T target) {
    return (T) compress(target, GzipCodec.class)
        .outputConf(AvroJob.OUTPUT_CODEC, DataFileConstants.DEFLATE_CODEC);
  }

  /**
   * Configure the given output target to be compressed using Snappy. If the Target is one of the AvroParquet targets
   * contained in Crunch, the Parquet-specific SnappyCodec will be used instead of the default Hadoop one.
   */
  public static <T extends Target> T snappy(T target) {
    Class<? extends CompressionCodec> snappyCodec = org.apache.hadoop.io.compress.SnappyCodec.class;
    if (target instanceof AvroParquetFileTarget || target instanceof AvroParquetFileSourceTarget) {
      snappyCodec = parquet.hadoop.codec.SnappyCodec.class;
    }
    return (T) compress(target, snappyCodec)
        .outputConf(AvroJob.OUTPUT_CODEC, DataFileConstants.SNAPPY_CODEC);
  }
}
