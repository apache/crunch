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
package org.apache.crunch.io.impl;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import com.google.common.collect.UnmodifiableIterator;
import org.apache.commons.io.IOUtils;

/**
 * Closes the wrapped {@code Closeable} when {@link #hasNext()} returns false.  As long a client loops through to
 * completion (doesn't abort early due to an exception, short circuit, etc.) resources will be closed automatically.
 */
public class AutoClosingIterator<T> extends UnmodifiableIterator<T> implements Closeable {
  private final Iterator<T> iter;
  private Closeable closeable;

  public AutoClosingIterator(Closeable closeable, Iterator<T> iter) {
    this.closeable = closeable;
    this.iter = iter;
  }

  @Override
  public boolean hasNext() {
    if (iter.hasNext()) {
      return true;
    } else {
      IOUtils.closeQuietly(this);
      return false;
    }
  }

  @Override
  public T next() {
    return iter.next();
  }

  @Override
  public void close() throws IOException {
    if (closeable != null) {
      closeable.close();
      closeable = null;
    }
  }
}
