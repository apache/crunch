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
package com.cloudera.crunch.util;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.crunch.impl.mr.run.CrunchRuntimeException;

/**
 * Functions for working with a job-specific distributed cache of objects, like the
 * serialized runtime nodes in a MapReduce.
 */
public class DistCache {
  
  public static void write(Configuration conf, Path path, Object value) throws IOException {
    ObjectOutputStream oos = new ObjectOutputStream(FileSystem.get(conf).create(path));
    oos.writeObject(value);
    oos.close();
    
    DistributedCache.addCacheFile(path.toUri(), conf);
  }
  
  public static Object read(Configuration conf, Path path) throws IOException {
    URI target = null;
    for (URI uri : DistributedCache.getCacheFiles(conf)) {
      if (uri.toString().equals(path.toString())) {
        target = uri;
        break;
      }
    }
    Object value = null;
    if (target != null) {
      Path targetPath = new Path(target.toString());
      ObjectInputStream ois = new ObjectInputStream(targetPath.getFileSystem(conf).open(targetPath));
      try {
        value = ois.readObject();
      } catch (ClassNotFoundException e) {
        throw new CrunchRuntimeException(e);
      }
      ois.close();
    }
    return value;
  }
}
