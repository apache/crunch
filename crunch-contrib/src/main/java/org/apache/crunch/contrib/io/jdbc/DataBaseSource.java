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

import java.io.IOException;
import java.sql.Driver;

import org.apache.crunch.Source;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/**
 * Source from reading from a database via a JDBC connection. Underlying
 * database reading is provided by {@link DBInputFormat}.
 * <p>
 * A type that is input via this class must be a Writable that also implements
 * DBWritable. On the {@link DBWritable#readFields(java.sql.ResultSet)} method
 * needs to be fully implemented form {@link DBWritable}.
 * 
 * @param <T> The input type of this source
 */
public class DataBaseSource<T extends DBWritable & Writable> implements Source<T> {

  private Class<T> inputClass;
  private PType<T> ptype;
  private String driverClass;
  private String url;
  private String username;
  private String password;
  private String selectClause;
  public String countClause;

  private DataBaseSource(Class<T> inputClass) {
    this.inputClass = inputClass;
    this.ptype = Writables.writables(inputClass);
  }

  static class Builder<T extends DBWritable & Writable> {

    private DataBaseSource<T> dataBaseSource;

    public Builder(Class<T> inputClass) {
      this.dataBaseSource = new DataBaseSource<T>(inputClass);
    }

    Builder<T> setDriverClass(Class<? extends Driver> driverClass) {
      dataBaseSource.driverClass = driverClass.getName();
      return this;
    }

    Builder<T> setUrl(String url) {
      dataBaseSource.url = url;
      return this;
    }

    Builder<T> setUsername(String username) {
      dataBaseSource.username = username;
      return this;
    }

    Builder<T> setPassword(String password) {
      dataBaseSource.password = password;
      return this;
    }

    Builder<T> selectSQLQuery(String selectClause) {
      dataBaseSource.selectClause = selectClause;
      return this;
    }

    Builder<T> countSQLQuery(String countClause) {
      dataBaseSource.countClause = countClause;
      return this;
    }

    DataBaseSource<T> build() {
      return dataBaseSource;
    }
  }

  @Override
  public void configureSource(Job job, int inputId) throws IOException {
    Configuration configuration = job.getConfiguration();
    DBConfiguration.configureDB(configuration, driverClass, url, username, password);
    job.setInputFormatClass(DBInputFormat.class);
    DBInputFormat.setInput(job, inputClass, selectClause, countClause);
  }

  @Override
  public long getSize(Configuration configuration) {
    // TODO Do something smarter here
    return 1000 * 1000;
  }

  @Override
  public PType<T> getType() {
    return ptype;
  }

}