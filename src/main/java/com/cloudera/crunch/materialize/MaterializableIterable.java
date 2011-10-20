package com.cloudera.crunch.materialize;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.io.ReadableSourceTarget;

public class MaterializableIterable<E> implements Iterable<E> {

  private static final Log LOG = LogFactory.getLog(MaterializableIterable.class);
  
  private final Pipeline pipeline;
  private final ReadableSourceTarget<E> sourceTarget;
  private Iterable<E> materialized;
  
  public MaterializableIterable(Pipeline pipeline, ReadableSourceTarget<E> source) {
	this.pipeline = pipeline;
	this.sourceTarget = source;
	this.materialized = null;
  }
  
  @Override
  public Iterator<E> iterator() {
	checkMaterialized();
	return materialized.iterator();
  }

  public void materialize() {
	try {
	  materialized = sourceTarget.read(pipeline.getConfiguration());
	} catch (IOException e) {
	  LOG.error("Could not materialize: " + sourceTarget, e);
	}	
  }

  private void checkMaterialized() {
	if (materialized == null) {
	  pipeline.run();
	  materialize();
	}
  }  
}
