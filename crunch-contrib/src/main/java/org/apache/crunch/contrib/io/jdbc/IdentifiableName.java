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
package org.apache.crunch.contrib.io.jdbc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class IdentifiableName implements DBWritable, Writable {

  public IntWritable id = new IntWritable();
  public Text name = new Text();

  @Override
  public void readFields(DataInput in) throws IOException {
    id.readFields(in);
    name.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    id.write(out);
    name.write(out);
  }

  @Override
  public void readFields(ResultSet resultSet) throws SQLException {
    id.set(resultSet.getInt(1));
    name.set(resultSet.getString(2));
  }

  @Override
  public void write(PreparedStatement preparedStatement) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }
}
