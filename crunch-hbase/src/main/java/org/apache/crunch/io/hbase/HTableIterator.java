/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.apache.crunch.io.hbase;

import org.apache.crunch.Pair;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

class HTableIterator implements Iterator<Pair<ImmutableBytesWritable, Result>> {
  private static final Logger LOG = LoggerFactory.getLogger(HTableIterator.class);

  private final Table table;
  private final Connection connection;
  private final Iterator<Scan> scans;
  private ResultScanner scanner;
  private Iterator<Result> iter;

  public HTableIterator(Connection connection, Table table, List<Scan> scans) {
    this.table = table;
    this.connection = connection;
    this.scans = scans.iterator();
    try{
      this.scanner = table.getScanner(this.scans.next());
    }catch(IOException ioe){
      throw new RuntimeException(ioe);
    }
    this.iter = scanner.iterator();
  }

  @Override
  public boolean hasNext() {
    boolean hasNext = iter.hasNext();
    if (!hasNext) {
      scanner.close();
      hasNext = scans.hasNext();
      if(hasNext){
        try{
          scanner = table.getScanner(this.scans.next());
          iter = scanner.iterator();
        } catch(IOException ioe){
          throw new RuntimeException("Unable to create next scanner from "+ table.getName(), ioe);
        }
      } else {
        try {
          table.close();
        } catch (IOException e) {
          LOG.error("Exception closing Table: {}", table.getName(), e);
        }
        try {
          connection.close();
        } catch (IOException e) {
          LOG.error("Exception closing Table: {}", table.getName(), e);
        }
      }
    }
    return hasNext;
  }

  @Override
  public Pair<ImmutableBytesWritable, Result> next() {
    Result next = iter.next();
    return Pair.of(new ImmutableBytesWritable(next.getRow()), next);
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
