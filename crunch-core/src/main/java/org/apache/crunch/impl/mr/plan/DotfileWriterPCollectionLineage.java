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
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.crunch.Target;
import org.apache.crunch.impl.dist.collect.PCollectionImpl;
import org.apache.crunch.impl.mr.collect.InputCollection;

/**
 * Writes <a href="http://www.graphviz.org">Graphviz</a> dot files to illustrate the topology of Crunch pipelines.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class DotfileWriterPCollectionLineage extends CommonDotfileWriter {

  private Map<PCollectionImpl<?>, Set<Target>> outputs;

  public DotfileWriterPCollectionLineage(Map<PCollectionImpl<?>, Set<Target>> outputs) {
    super();
    this.outputs = outputs;
  }

  private void formatPCollectionLineage(PCollectionImpl pcollection, String color) {

    contentBuilder.append(formatPCollection(pcollection));

    // for input pcollections add the related source and link it to the collection
    if (pcollection instanceof InputCollection) {
      InputCollection ic = (InputCollection) pcollection;

      formatSource(ic.getSource(), DEFAULT_FOLDER_COLOR);

      link(ic.getSource(), pcollection, color);
    }

    List<PCollectionImpl<?>> parents = pcollection.getParents();
    if (!CollectionUtils.isEmpty(parents)) {
      for (PCollectionImpl parentPCollection : parents) {
        link(parentPCollection, pcollection, color);
        formatPCollectionLineage(parentPCollection, color);
      }
    }
  }

  @Override
  protected void doBuildDiagram() {

    int outputIndex = 0;

    for (PCollectionImpl<?> pcollection : outputs.keySet()) {

      String pathColor = COLORS[outputIndex++];

      formatPCollectionLineage(pcollection, pathColor);

      for (Target target : outputs.get(pcollection)) {
        formatTarget(target, DEFAULT_FOLDER_COLOR);
        link(pcollection, target, pathColor);
      }
    }
  }

  @Override
  protected void doGetLegend(StringBuilder lsb) {
    lsb.append("\"Folder\"  [label=\"Folder Name\" fontsize=10 shape=folder color=darkGreen]\n").append(
        "\"PCollection\"  [label=\"{PCollection Name | PCollection Class| PType }\" fontsize=10 shape=record]\n");
  }
}
