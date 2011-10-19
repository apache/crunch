package com.cloudera.crunch.materialize;

import java.io.IOException;
import java.util.Iterator;

import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.io.ReadableSourceTarget;

public class CrunchIterable<E> implements Iterable<E> {

  private final Pipeline pipeline;
  private final ReadableSourceTarget<E> sourceTarget;
  private Iterable<E> materialized;
  
  public CrunchIterable(Pipeline pipeline, ReadableSourceTarget<E> source) {
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
	  // TODO Auto-generated catch block
	  e.printStackTrace();
	}	
  }

  private void checkMaterialized() {
	if (materialized == null) {
	  pipeline.run();
	  materialize();
	}
  }  
}
