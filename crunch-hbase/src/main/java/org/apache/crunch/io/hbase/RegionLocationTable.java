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

import javax.annotation.Nullable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Provides lookup functionality for the region server location for row keys in an HBase table.
 * <p>
 * This is a helper class to optimize the locality of HFiles created with {@link HFileOutputFormatForCrunch}, by
 * specifying the name of the region server which is hosting the region of a given row as the preferred HDFS data node
 * for hosting the written HFile. This is intended to ensure that bulk-created HFiles will be available on the local
 * filesystem on the region servers using the created HFile, thus allowing short-circuit reads to the local file system
 * on the bulk-created HFiles.
 */
class RegionLocationTable {

  /**
   * Per-output configuration key which contains the path to a serialized region location table.
   */
  public static final String REGION_LOCATION_TABLE_PATH = "crunch.hfileregionlocation.path";

  private final String tableName;
  private final NavigableMap<byte[], String> regionStartToServerHostName;

  public static RegionLocationTable create(String tableName, List<HRegionLocation> regionLocationList) {
    NavigableMap<byte[], String> regionStartToServerHostName = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (HRegionLocation regionLocation : regionLocationList) {
      HRegionInfo regionInfo = regionLocation.getRegionInfo();
      if (regionInfo == null) {
        continue;
      }
      byte[] startKey = regionInfo.getStartKey();
      if (startKey == null) {
        startKey = HConstants.EMPTY_START_ROW;
      }
      ServerName serverName = regionLocation.getServerName();
      if (serverName != null) {
        regionStartToServerHostName.put(startKey, serverName.getHostname());
      }
    }
    return new RegionLocationTable(tableName, regionStartToServerHostName);
  }

  private RegionLocationTable(String tableName,
      NavigableMap<byte[], String> regionStartToServerHostName) {
    this.tableName = tableName;
    this.regionStartToServerHostName = regionStartToServerHostName;
  }

  /**
   * Returns the name of the HBase table to which this region location table applies.
   *
   * @return name of the related HBase table
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Returns the optional preferred node for a row.
   * <p>
   * The return value of this method is an {@link InetSocketAddress} to be in line with the HFile API (and
   * underlying HDFS API) which use InetSocketAddress. The port number is always 0 on the returned InetSocketAddress,
   * as it is not known from outside the scope of a region server. The HDFS API is implemented to deal "correctly"
   * with this, mapping host name to a random data node on the same machine, which is sufficient for the purposes
   * here.
   * <p>
   * The return value will be null if no preferred node is known for the given row.
   *
   * @param rowKey row key of the row for which the preferred node is to be calculated
   * @return socket address of the preferred storage node for the given row, or null
   */
  @Nullable
  public InetSocketAddress getPreferredNodeForRow(byte[] rowKey) {
    Map.Entry<byte[], String> matchingEntry = regionStartToServerHostName.floorEntry(rowKey);
    if (matchingEntry != null) {
      return InetSocketAddress.createUnresolved(matchingEntry.getValue(), 0);
    } else {
      return null;
    }
  }

  /**
   * Serialize this table to a {@link DataOutput}. The serialized value can be deserialized via the
   * {@link #deserialize(DataInput)} method.
   *
   * @param dataOutput output to which the table is to be serialized
   */
  public void serialize(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(tableName);
    dataOutput.writeInt(regionStartToServerHostName.size());
    for (Map.Entry<byte[], String> regionToHostEntry : regionStartToServerHostName.entrySet()) {
      byte[] rowKey = regionToHostEntry.getKey();
      dataOutput.writeInt(rowKey.length);
      dataOutput.write(rowKey);
      dataOutput.writeUTF(regionToHostEntry.getValue());
    }
  }

  /**
   * Deserialize a table which was serialized to with the {@link #serialize(DataOutput)} method.
   *
   * @param dataInput input containing a serialized instance of this class
   * @return the deserialized table
   */
  public static RegionLocationTable deserialize(DataInput dataInput) throws IOException {
    NavigableMap<byte[], String> regionStartToServerHostName = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    String tableName = dataInput.readUTF();
    int numEntries = dataInput.readInt();
    for (int i = 0; i < numEntries; i++) {
      int rowKeyLength = dataInput.readInt();
      byte[] rowKey = new byte[rowKeyLength];
      dataInput.readFully(rowKey, 0, rowKeyLength);
      String hostName = dataInput.readUTF();
      regionStartToServerHostName.put(rowKey, hostName);
    }
    return new RegionLocationTable(tableName, regionStartToServerHostName);
  }
}
