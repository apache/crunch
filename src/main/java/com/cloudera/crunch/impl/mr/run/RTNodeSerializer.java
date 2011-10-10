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
package com.cloudera.crunch.impl.mr.run;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.cloudera.crunch.impl.mr.plan.DoNode;
import com.google.common.collect.Lists;

public class RTNodeSerializer {

  private static final Log LOG = LogFactory.getLog(RTNodeSerializer.class);
  
  public void serialize(List<DoNode> nodes, Configuration conf,
      NodeContext context) throws IOException {
    List<RTNode> rtNodes = Lists.newArrayList();
    for (DoNode node : nodes) {
      rtNodes.add(node.toRTNode(true, conf, context));
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(rtNodes);
    String serialized = Base64.encodeBase64String(baos.toByteArray());
    conf.set(context.getConfigurationKey(), serialized);
  }

  @SuppressWarnings("unchecked")
  public List<RTNode> deserialize(CrunchTaskContext context) throws IOException {
    String serializedString = context.getSerializedState();
    byte[] serialized = Base64.decodeBase64(serializedString);
    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(
        serialized));
    List<RTNode> nodes = null;
    try {
      nodes = (List<RTNode>) ois.readObject();
    } catch (ClassNotFoundException e) {
      LOG.info("Crunch deserialization error", e);
      throw new CrunchRuntimeException(e);
    }
    if (nodes != null) {
      for (RTNode node : nodes) {
        node.initialize(context);
      }
    }
    return nodes;
  }

}
