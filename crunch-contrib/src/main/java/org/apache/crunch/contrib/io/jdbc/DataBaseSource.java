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

import java.sql.Driver;

import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.impl.FileSourceImpl;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
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
public class DataBaseSource<T extends DBWritable & Writable> extends FileSourceImpl<T> {

  private DataBaseSource(Class<T> inputClass,
      String driverClassName,
      String url,
      String username,
      String password,
      String selectClause,
      String countClause) {
    super(
        new Path("dbsource"),
        Writables.writables(inputClass),
        FormatBundle.forInput(DBInputFormat.class)
            .set(DBConfiguration.DRIVER_CLASS_PROPERTY, driverClassName)
            .set(DBConfiguration.URL_PROPERTY, url)
            .set(DBConfiguration.USERNAME_PROPERTY, username)
            .set(DBConfiguration.PASSWORD_PROPERTY, password)
            .set(DBConfiguration.INPUT_CLASS_PROPERTY, inputClass.getCanonicalName())
            .set(DBConfiguration.INPUT_QUERY, selectClause)
            .set(DBConfiguration.INPUT_COUNT_QUERY, countClause));
  }

  public static class Builder<T extends DBWritable & Writable> {

    private Class<T> inputClass;
    private String driverClass;
    private String url;
    private String username;
    private String password;
    private String selectClause;
    public String countClause;

    public Builder(Class<T> inputClass) {
      this.inputClass = inputClass;
    }

    Builder<T> setDriverClass(Class<? extends Driver> driverClass) {
      this.driverClass = driverClass.getName();
      return this;
    }

    Builder<T> setUrl(String url) {
      this.url = url;
      return this;
    }

    Builder<T> setUsername(String username) {
      this.username = username;
      return this;
    }

    Builder<T> setPassword(String password) {
      this.password = password;
      return this;
    }

    Builder<T> selectSQLQuery(String selectClause) {
      this.selectClause = selectClause;
      return this;
    }

    Builder<T> countSQLQuery(String countClause) {
      this.countClause = countClause;
      return this;
    }

    DataBaseSource<T> build() {
      return new DataBaseSource<T>(
          inputClass,
          driverClass,
          url,
          username,
          password,
          selectClause,
          countClause);
    }
  }

  @Override
  public long getSize(Configuration configuration) {
    // TODO Do something smarter here
    return 1000 * 1000;
  }

  @Override
  public long getLastModifiedAt(Configuration configuration) {
    return -1;
  }
}
