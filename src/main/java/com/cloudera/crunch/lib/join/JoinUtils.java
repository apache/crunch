/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.crunch.lib.join;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryData;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.cloudera.crunch.types.PTypeFamily;
import com.cloudera.crunch.types.writable.TupleWritable;
import com.cloudera.crunch.types.writable.WritableTypeFamily;

/**
 * Utilities that are useful in joining multiple data sets via a MapReduce.
 *
 */
public class JoinUtils {

  public static Class<? extends Partitioner> getPartitionerClass(PTypeFamily typeFamily) {
    if (typeFamily == WritableTypeFamily.getInstance()) {
      return TupleWritablePartitioner.class;
    } else {
      return AvroIndexedRecordPartitioner.class;
    }
  }
  
  public static Class<? extends RawComparator> getGroupingComparator(PTypeFamily typeFamily) {
    if (typeFamily == WritableTypeFamily.getInstance()) {
      return TupleWritableComparator.class;
    } else {
      return AvroPairGroupingComparator.class;
    }
  }
  
  public static class TupleWritablePartitioner extends Partitioner<TupleWritable, Writable> {
    @Override
    public int getPartition(TupleWritable key, Writable value, int numPartitions) {
      return (Math.abs(key.get(0).hashCode()) & Integer.MAX_VALUE) % numPartitions;
    }
  }
  
  public static class TupleWritableComparator implements RawComparator<TupleWritable> {
    
    private DataInputBuffer buffer = new DataInputBuffer();
    private TupleWritable key1 = new TupleWritable();
    private TupleWritable key2 = new TupleWritable();
    
    @Override
    public int compare(TupleWritable o1, TupleWritable o2) {
      return ((WritableComparable)o1.get(0)).compareTo((WritableComparable)o2.get(0));
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      try {
        buffer.reset(b1, s1, l1);
        key1.readFields(buffer);
      
        buffer.reset(b2, s2, l2);
        key2.readFields(buffer);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      
      return compare(key1, key2);
    }
  }
  
  public static class AvroIndexedRecordPartitioner extends Partitioner<AvroKey, AvroValue> {
    @Override
    public int getPartition(AvroKey key, AvroValue value, int numPartitions) {
      IndexedRecord record = (IndexedRecord) key.datum();
      return (Math.abs(record.get(0).hashCode()) & Integer.MAX_VALUE) % numPartitions;
    }
  }
  
  public static class AvroPairGroupingComparator<T> extends Configured implements RawComparator<AvroWrapper<T>> {
    private Schema schema;

    @Override
    public void setConf(Configuration conf) {
      super.setConf(conf);
      if (conf != null) {
        Schema mapOutputSchema = AvroJob.getMapOutputSchema(conf);
        Schema keySchema = org.apache.avro.mapred.Pair.getKeySchema(mapOutputSchema);
        schema = keySchema.getFields().get(0).schema();
      }
    }
    
    @Override
    public int compare(AvroWrapper<T> x, AvroWrapper<T> y) {
      return ReflectData.get().compare(x.datum(), y.datum(), schema);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return BinaryData.compare(b1, s1, l1, b2, s2, l2, schema);
    }
  }
}
