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
package org.apache.crunch.io.orc;

import java.util.Iterator;

import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.MapFn;
import org.apache.crunch.io.FileReaderFactory;
import org.apache.crunch.types.PType;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.google.common.collect.UnmodifiableIterator;

public class OrcFileReaderFactory<T> implements FileReaderFactory<T> {
  
  private MapFn<Object, T> inputFn;
  private OrcInputFormat inputFormat = new OrcInputFormat();
  private int[] readColumns;
  
  public OrcFileReaderFactory(PType<T> ptype) {
    this(ptype, null);
  }
  
  public OrcFileReaderFactory(PType<T> ptype, int[] readColumns) {
    inputFn = ptype.getInputMapFn();
    this.readColumns = readColumns;
  }

  @Override
  public Iterator<T> read(FileSystem fs, final Path path) {
    try {
      if (!fs.isFile(path)) {
        throw new CrunchRuntimeException("Not a file: " + path);
      }
      
      inputFn.initialize();
      
      FileStatus status = fs.getFileStatus(path);
      FileSplit split = new FileSplit(path, 0, status.getLen(), new String[0]);
      
      JobConf conf = new JobConf();
      if (readColumns != null) {
        conf.setBoolean(OrcFileSource.HIVE_READ_ALL_COLUMNS, false);
        conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, OrcFileSource.getColumnIdsStr(readColumns));
      }
      final RecordReader<NullWritable, OrcStruct> reader = inputFormat.getRecordReader(split, conf, Reporter.NULL);
      
      return new UnmodifiableIterator<T>() {
        
        private boolean checked = false;
        private boolean hasNext;
        private OrcStruct value;
        private OrcWritable writable = new OrcWritable();

        @Override
        public boolean hasNext() {
          try {
            if (value == null) {
              value = reader.createValue();
            }
            if (!checked) {
              hasNext = reader.next(NullWritable.get(), value);
              checked = true;
            }
            return hasNext;
          } catch (Exception e) {
            throw new CrunchRuntimeException("Error while reading local file: " + path, e);
          }
        }

        @Override
        public T next() {
          try {
            if (value == null) {
              value = reader.createValue();
            }
            if (!checked) {
              reader.next(NullWritable.get(), value);
            }
            checked = false;
            writable.set(value);
            return inputFn.map(writable);
          } catch (Exception e) {
            throw new CrunchRuntimeException("Error while reading local file: " + path, e);
          }
        }
        
      };
    } catch (Exception e) {
      throw new CrunchRuntimeException("Error while reading local file: " + path, e);
    }
  }

}
