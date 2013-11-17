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
package org.apache.crunch.impl.mr.plan;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.crunch.Source;
import org.apache.crunch.impl.dist.collect.BaseGroupedTable;
import org.apache.crunch.impl.dist.collect.BaseInputCollection;
import org.apache.crunch.impl.dist.collect.PCollectionImpl;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 *
 */
class Vertex {
  private final PCollectionImpl impl;
  
  private boolean output;
  private Set<Edge> incoming;
  private Set<Edge> outgoing;
  
  public Vertex(PCollectionImpl impl) {
    this.impl = impl;
    this.incoming = Sets.newHashSet();
    this.outgoing = Sets.newHashSet();
  }
  
  public PCollectionImpl getPCollection() {
    return impl;
  }
  
  public boolean isInput() {
    return impl instanceof BaseInputCollection;
  }
  
  public boolean isGBK() {
    return impl instanceof BaseGroupedTable;
  }
  
  public void setOutput() {
    this.output = true;
  }
  
  public boolean isOutput() {
    return output;
  }
  
  public Source getSource() {
    if (isInput()) {
      return ((BaseInputCollection) impl).getSource();
    }
    return null;
  }
  
  public void addIncoming(Edge edge) {
    this.incoming.add(edge);
  }
  
  public void addOutgoing(Edge edge) {
    this.outgoing.add(edge);
  }
  
  public List<Vertex> getAllNeighbors() {
    List<Vertex> n = Lists.newArrayList();
    for (Edge e : incoming) {
      n.add(e.getHead());
    }
    for (Edge e : outgoing) {
      n.add(e.getTail());
    }
    return n;
  }
  
  public Set<Edge> getIncomingEdges() {
    return incoming;
  }
  
  public Set<Edge> getOutgoingEdges() {
    return outgoing;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof Vertex)) {
      return false;
    }
    Vertex other = (Vertex) obj;
    return impl.equals(other.impl);
  }
  
  @Override
  public int hashCode() {
    return 17 + 37 * impl.hashCode();
  }

  @Override
  public String toString() {
    return new ReflectionToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).setExcludeFieldNames(
        new String[] { "outgoing", "incoming" }).toString();
  }
}
