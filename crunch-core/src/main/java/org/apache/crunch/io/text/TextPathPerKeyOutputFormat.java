/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.crunch.io.text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;

public class TextPathPerKeyOutputFormat<V> extends TextOutputFormat<Text, V> {
    @Override
    public RecordWriter<Text, V> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException {
        Configuration conf = taskAttemptContext.getConfiguration();

        FileOutputCommitter outputCommitter = (FileOutputCommitter) getOutputCommitter(taskAttemptContext);
        Path basePath = new Path(outputCommitter.getWorkPath(), conf.get("mapreduce.output.basename", "part"));

        boolean isCompressed = FileOutputFormat.getCompressOutput(taskAttemptContext);
        CompressionCodec codec = null;
        String extension = "";

        if (isCompressed) {
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(taskAttemptContext, GzipCodec.class);
            codec = ReflectionUtils.newInstance(codecClass, conf);
            extension = codec.getDefaultExtension();
        }

        return new TextPathPerKeyRecordWriter<>(basePath, getUniqueFile(taskAttemptContext, "part", extension),
                isCompressed, codec, taskAttemptContext);
    }

    private class TextPathPerKeyRecordWriter<V> extends RecordWriter<Text, V> {

        private final Path basePath;
        private final String uniqueFileName;
        private final Configuration conf;
        private String currentKey;
        private RecordWriter<V, NullWritable> currentWriter;
        private CompressionCodec compressionCodec;
        private boolean isCompressed;
        private TaskAttemptContext taskAttemptContext;

        public TextPathPerKeyRecordWriter(Path basePath, String uniqueFileName, boolean isCompressed,
                                          CompressionCodec codec, TaskAttemptContext context) {
            this.basePath = basePath;
            this.uniqueFileName = uniqueFileName;
            this.conf = context.getConfiguration();
            this.isCompressed = isCompressed;
            this.compressionCodec = codec;
            this.taskAttemptContext = context;
        }

        @Override
        public void write(Text record, V n) throws IOException, InterruptedException {
            String key = record.toString();
            if (!key.equals(currentKey)) {
                if (currentWriter != null) {
                    currentWriter.close(taskAttemptContext);
                }

                currentKey = key;
                Path dir = new Path(basePath, key);
                FileSystem fs = dir.getFileSystem(conf);
                if (!fs.exists(dir)) {
                    fs.mkdirs(dir);
                }

                Path filePath = new Path(dir, uniqueFileName);

                DataOutputStream dataOutputStream;

                if (fs.exists(filePath)) {
                    dataOutputStream = fs.append(filePath);
                } else {
                    dataOutputStream = fs.create(filePath);
                }

                if (isCompressed && compressionCodec != null) {
                    dataOutputStream = new DataOutputStream(compressionCodec.createOutputStream(dataOutputStream));
                }

                String keyValueSeparator = conf.get(SEPERATOR, "\t");
                currentWriter = new LineRecordWriter<>(dataOutputStream, keyValueSeparator);
            }

            currentWriter.write(n, NullWritable.get());
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if (currentWriter != null) {
                currentWriter.close(taskAttemptContext);
                currentKey = null;
                currentWriter = null;
            }
        }
    }
}
