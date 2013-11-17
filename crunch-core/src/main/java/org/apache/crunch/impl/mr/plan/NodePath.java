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

import java.util.Iterator;
import java.util.LinkedList;

import org.apache.crunch.impl.dist.collect.PCollectionImpl;

import com.google.common.collect.Lists;

class NodePath implements Iterable<PCollectionImpl<?>> {
  private LinkedList<PCollectionImpl<?>> path;

  public NodePath() {
    this.path = Lists.newLinkedList();
  }

  public NodePath(PCollectionImpl<?> tail) {
    this.path = Lists.newLinkedList();
    this.path.add(tail);
  }

  public NodePath(NodePath other) {
    this.path = Lists.newLinkedList(other.path);
  }

  public void push(PCollectionImpl<?> stage) {
    this.path.push(stage);
  }

  public NodePath close(PCollectionImpl<?> head) {
    this.path.push(head);
    return this;
  }

  public Iterator<PCollectionImpl<?>> iterator() {
    return path.iterator();
  }

  public Iterator<PCollectionImpl<?>> descendingIterator() {
    return path.descendingIterator();
  }

  public PCollectionImpl<?> get(int index) {
    return path.get(index);
  }

  public PCollectionImpl<?> head() {
    return path.peekFirst();
  }

  public PCollectionImpl<?> tail() {
    return path.peekLast();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof NodePath)) {
      return false;
    }
    NodePath nodePath = (NodePath) other;
    return path.equals(nodePath.path);
  }

  @Override
  public int hashCode() {
    return 17 + 37 * path.hashCode();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (PCollectionImpl<?> collect : path) {
      sb.append(collect.getName() + "|");
    }
    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }

  public NodePath splitAt(int splitIndex, PCollectionImpl<?> newHead) {
    NodePath top = new NodePath();
    for (int i = 0; i <= splitIndex; i++) {
      top.path.add(path.get(i));
    }
    LinkedList<PCollectionImpl<?>> nextPath = Lists.newLinkedList();
    nextPath.add(newHead);
    nextPath.addAll(path.subList(splitIndex + 1, path.size()));
    path = nextPath;
    return top;
  }
  
  public NodePath splitAt(PCollectionImpl split, PCollectionImpl<?> newHead) {
    NodePath top = new NodePath();
    int splitIndex = 0;
    for (PCollectionImpl p : path) {
      top.path.add(p);
      if (p == split) {
        break;
      }
      splitIndex++;
    }
    LinkedList<PCollectionImpl<?>> nextPath = Lists.newLinkedList();
    nextPath.add(newHead);
    nextPath.addAll(path.subList(splitIndex + 1, path.size()));
    path = nextPath;
    return top;
  }
}