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

import static org.junit.Assert.assertEquals;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.contrib.io.jdbc.DataBaseSource;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.test.CrunchTestSupport;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.h2.tools.RunScript;
import org.h2.tools.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class DataBaseSourceIT extends CrunchTestSupport implements Serializable {
  private transient Server server;

  @Before
  public void start() throws Exception {
    File file = tempDir.copyResourceFile("data.script");
    server = Server.createTcpServer().start();
    RunScript.execute("jdbc:h2:tcp://localhost/~/test", "sa", "", file.getAbsolutePath(), "utf-8", false);
  }

  @After
  public void stop() throws Exception {
    server.stop();
  }

  public static class IdentifiableName implements DBWritable, Writable {

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

  @Test
  public void testReadFromSource() throws Exception {
    Pipeline pipeline = new MRPipeline(DataBaseSourceIT.class);
    DataBaseSource<IdentifiableName> dbsrc = new DataBaseSource.Builder<IdentifiableName>(IdentifiableName.class)
        .setDriverClass(org.h2.Driver.class)
        .setUrl("jdbc:h2:tcp://localhost/~/test").setUsername("sa").setPassword("")
        .selectSQLQuery("SELECT ID, NAME FROM TEST").countSQLQuery("select count(*) from Test").build();

    PCollection<IdentifiableName> cdidata = pipeline.read(dbsrc);
    PCollection<String> names = cdidata.parallelDo(new DoFn<IdentifiableName, String>() {

      @Override
      public void process(IdentifiableName input, Emitter<String> emitter) {
        emitter.emit(input.name.toString());
      }

    }, Writables.strings());

    List<String> nameList = Lists.newArrayList(names.materialize());
    pipeline.done();

    assertEquals(2, nameList.size());
    assertEquals(Sets.newHashSet("Hello", "World"), Sets.newHashSet(nameList));

  }
}