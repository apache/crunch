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
package org.apache.crunch.lib.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;

public class ReverseWritableComparator<T> extends Configured implements RawComparator<T> {

  private RawComparator<T> comparator;

  @SuppressWarnings("unchecked")
  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf != null) {
      JobConf jobConf = new JobConf(conf);
      comparator = WritableComparator.get(jobConf.getMapOutputKeyClass().asSubclass(WritableComparable.class));
    }
  }

  @Override
  public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5) {
    return safeNegate(comparator.compare(arg0, arg1, arg2, arg3, arg4, arg5));
  }

  @Override
  public int compare(T o1, T o2) {
    return safeNegate(comparator.compare(o1, o2));
  }

  /**
   * @return an {@code int} definitely of the opposite sign as its argument. This is {@code -i}
   *  unless {@code i == Integer.MIN_VALUE} in which case it's {@code Integer.MAX_VALUE}
   */
  private static int safeNegate(int i) {
    return i == Integer.MIN_VALUE ? Integer.MAX_VALUE : -i;
  }

}