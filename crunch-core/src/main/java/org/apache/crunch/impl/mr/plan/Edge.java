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

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.crunch.Target;
import org.apache.crunch.impl.dist.collect.BaseGroupedTable;
import org.apache.crunch.impl.dist.collect.PCollectionImpl;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

class Edge {
  private final Vertex head;
  private final Vertex tail;
  private final Set<NodePath> paths;
  
  Edge(Vertex head, Vertex tail) {
    this.head = head;
    this.tail = tail;
    this.paths = Sets.newTreeSet(NODE_CMP);
  }
  
  public Vertex getHead() {
    return head;
  }
  
  public Vertex getTail() {
    return tail;
  }

  public void addNodePath(NodePath path) {
    this.paths.add(path);
  }
  
  public void addAllNodePaths(Collection<NodePath> paths) {
    this.paths.addAll(paths);
  }
  
  public Set<NodePath> getNodePaths() {
    return paths;
  }

  public Map<NodePath,  PCollectionImpl> getSplitPoints(boolean breakpointsOnly) {
    List<NodePath> np = Lists.newArrayList(paths);
    List<PCollectionImpl<?>> smallestOverallPerPath = Lists.newArrayListWithExpectedSize(np.size());
    Map<PCollectionImpl<?>, Set<Integer>> pathCounts = Maps.newTreeMap(PCOL_CMP);
    Map<NodePath, PCollectionImpl> splitPoints = Maps.newHashMap();
    for (int i = 0; i < np.size(); i++) {
      long bestSize = Long.MAX_VALUE;
      boolean breakpoint = false;
      PCollectionImpl<?> best = null;
      for (PCollectionImpl<?> pc : np.get(i)) {
        if (!(pc instanceof BaseGroupedTable) && (!breakpointsOnly || pc.isBreakpoint())) {
          if (pc.isBreakpoint()) {
            if (!breakpoint || pc.getSize() < bestSize) {
              best = pc;
              bestSize = pc.getSize();
              breakpoint = true;
            }
          } else if (!breakpoint && pc.getSize() < bestSize) {
            best = pc;
            bestSize = pc.getSize();
          }
          Set<Integer> cnts = pathCounts.get(pc);
          if (cnts == null) {
            cnts = Sets.newHashSet();
            pathCounts.put(pc, cnts);
          }
          cnts.add(i);
        }
      }
      smallestOverallPerPath.add(best);
      if (breakpoint) {
        splitPoints.put(np.get(i), best);
      }
    }

    Set<Integer> missing = Sets.newHashSet();
    for (int i = 0; i < np.size(); i++) {
      if (!splitPoints.containsKey(np.get(i))) {
        missing.add(i);
      }
    }

    if (breakpointsOnly && missing.size() > 0) {
      // We can't create new splits in this mode
      return ImmutableMap.of();
    } else if (missing.isEmpty()) {
      return splitPoints;
    } else {
      // Need to either choose the smallest collection from each missing path,
      // or the smallest single collection that is on all paths as the split target.
      Set<PCollectionImpl<?>> smallest = Sets.newHashSet();
      long smallestSize = 0;
      for (Integer id : missing) {
        PCollectionImpl<?> s = smallestOverallPerPath.get(id);
        if (!smallest.contains(s)) {
          smallest.add(s);
          smallestSize += s.getSize();
        }
      }

      PCollectionImpl<?> singleBest = null;
      long singleSmallestSize = Long.MAX_VALUE;
      for (Map.Entry<PCollectionImpl<?>, Set<Integer>> e : pathCounts.entrySet()) {
        if (Sets.difference(missing, e.getValue()).isEmpty() && e.getKey().getSize() < singleSmallestSize) {
          singleBest = e.getKey();
          singleSmallestSize = singleBest.getSize();
        }
      }

      if (smallestSize < singleSmallestSize) {
        for (Integer id : missing) {
          splitPoints.put(np.get(id), smallestOverallPerPath.get(id));
        }
      } else {
        for (Integer id : missing) {
          splitPoints.put(np.get(id), singleBest);
        }
      }
    }
    return splitPoints;
  }
  
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Edge)) {
      return false;
    }
    Edge e = (Edge) other;
    return head.equals(e.head) && tail.equals(e.tail) && paths.equals(e.paths);
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(head).append(tail).toHashCode();
  }
  
  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }

  private static Comparator<NodePath> NODE_CMP = new Comparator<NodePath>() {
    @Override
    public int compare(NodePath left, NodePath right) {
      if (left == right || left.equals(right)) {
        return 0;
      }
      return left.toString().compareTo(right.toString());
    }
  };

  private static Comparator<PCollectionImpl<?>> PCOL_CMP = new Comparator<PCollectionImpl<?>>() {
    @Override
    public int compare(PCollectionImpl<?> left, PCollectionImpl<?> right) {
      if (left == right || left.equals(right)) {
        return 0;
      }
      String leftName = left.getName();
      String rightName = right.getName();
      if (leftName == null || rightName == null || leftName.equals(rightName)) {
        return left.hashCode() < right.hashCode() ? -1 : 1;
      }
      return leftName.compareTo(rightName);
    }
  };
}
