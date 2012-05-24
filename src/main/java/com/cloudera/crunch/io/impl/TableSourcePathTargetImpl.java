/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
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
package com.cloudera.crunch.io.impl;

import com.cloudera.crunch.Pair;
import com.cloudera.crunch.TableSource;
import com.cloudera.crunch.io.PathTarget;
import com.cloudera.crunch.types.PTableType;

public class TableSourcePathTargetImpl<K, V> extends SourcePathTargetImpl<Pair<K, V>>
	implements TableSource<K, V> {

  public TableSourcePathTargetImpl(TableSource<K, V> source, PathTarget target) {
	super(source, target);
  }
  
  @Override
  public PTableType<K, V> getTableType() {
	return ((TableSource<K, V>) source).getTableType();
  }
}
