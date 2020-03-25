/*
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

package org.apache.crunch.io.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.junit.Before;
import org.junit.Test;

public class RegionLocationTableTest {

  private static final String TABLE_NAME = "namespace:DATA_TABLE";
  private RegionLocationTable regionLocationTable;

  @Before
  public void setUp() {
    regionLocationTable = RegionLocationTable.create(TABLE_NAME,
        ImmutableList.of(
            location(null, new byte[] { 10 }, "serverA"),
            location(new byte[] { 10 }, new byte[] { 20 }, "serverB"),
            location(new byte[] { 20 }, new byte[] { 30 }, "serverC"),
            location(new byte[] { 30 }, null, "serverD")));
  }

  @Test
  public void testLookupRowInFirstRegion() {
    assertEquals(
        InetSocketAddress.createUnresolved("serverA", 0),
        regionLocationTable.getPreferredNodeForRow(new byte[] { 5 }));
  }

  @Test
  public void testLookupRowInNonBoundaryRegion() {
    assertEquals(
        InetSocketAddress.createUnresolved("serverC", 0),
        regionLocationTable.getPreferredNodeForRow(new byte[] { 25 }));
  }

  @Test
  public void testLookupRowInLastRegion() {
    assertEquals(
        InetSocketAddress.createUnresolved("serverD", 0),
        regionLocationTable.getPreferredNodeForRow(new byte[] { 35 }));
  }

  @Test
  public void testLookupRowOnRegionBoundary() {
    assertEquals(
        InetSocketAddress.createUnresolved("serverB", 0),
        regionLocationTable.getPreferredNodeForRow(new byte[] { 10 }));
  }

  @Test
  public void testEmpty() {
    RegionLocationTable emptyTable = RegionLocationTable.create(TABLE_NAME,
        ImmutableList.<HRegionLocation>of());

    assertNull(
        emptyTable.getPreferredNodeForRow(new byte[] { 10 }));
  }

  @Test
  public void testSerializationRoundTrip() throws IOException {
    ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
    DataOutput dataOutput = new DataOutputStream(byteOutputStream);

    regionLocationTable.serialize(dataOutput);

    ByteArrayInputStream byteInputStream = new ByteArrayInputStream(byteOutputStream.toByteArray());
    DataInput dataInput = new DataInputStream(byteInputStream);

    RegionLocationTable deserialized = RegionLocationTable.deserialize(dataInput);

    // Just a basic test to make sure it works as before
    assertEquals(
        InetSocketAddress.createUnresolved("serverA", 0),
        deserialized.getPreferredNodeForRow(new byte[] { 5 }));
  }

  @Test
  public void testSerializationRoundTrip_EmptyTable() throws IOException {
    ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
    DataOutput dataOutput = new DataOutputStream(byteOutputStream);

    RegionLocationTable emptyTable = RegionLocationTable.create(TABLE_NAME,
        ImmutableList.<HRegionLocation>of());

    emptyTable.serialize(dataOutput);

    ByteArrayInputStream byteInputStream = new ByteArrayInputStream(byteOutputStream.toByteArray());
    DataInput dataInput = new DataInputStream(byteInputStream);

    RegionLocationTable deserialized = RegionLocationTable.deserialize(dataInput);

    // Just a basic test to make sure it works as before
    assertNull(
        deserialized.getPreferredNodeForRow(new byte[] { 10 }));
  }

  @Test
  public void testNullRegionInfo() {
    RegionLocationTable table = RegionLocationTable.create(TABLE_NAME,
        ImmutableList.of(location(null, serverName("serverA"))));
    assertNull(
        table.getPreferredNodeForRow(new byte[] { 15 }));
  }

  @Test
  public void testNullServerName() {
    RegionLocationTable table = RegionLocationTable.create(TABLE_NAME,
        ImmutableList.of(location(regionInfo(new byte[] { 10 }, new byte[] { 20 }), null)));
    assertNull(
        table.getPreferredNodeForRow(new byte[] { 15 }));
  }

  private static HRegionLocation location(byte[] startKey, byte[] endKey, String hostName) {
    return location(regionInfo(startKey, endKey), serverName(hostName));
  }

  private static HRegionLocation location(HRegionInfo regionInfo, ServerName serverName) {
    return new HRegionLocation(regionInfo, serverName);
  }

  private static HRegionInfo regionInfo(byte[] startKey, byte[] endKey) {
    return new HRegionInfo(TableName.valueOf(TABLE_NAME), startKey, endKey);
  }

  private static ServerName serverName(String hostName) {
    return ServerName.valueOf(hostName, 60020, System.currentTimeMillis());
  }

}