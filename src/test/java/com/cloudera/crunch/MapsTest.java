package com.cloudera.crunch;

import java.util.Map;

import org.junit.Test;

import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.test.FileHelper;
import com.cloudera.crunch.types.PTypeFamily;
import com.cloudera.crunch.types.avro.AvroTypeFamily;
import com.cloudera.crunch.types.writable.WritableTypeFamily;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class MapsTest {

  @Test
  public void testWritables() throws Exception {
	run(WritableTypeFamily.getInstance());
  }
  
  @Test
  public void testAvros() throws Exception {
	run(AvroTypeFamily.getInstance());
  }
  
  public static void run(PTypeFamily typeFamily) throws Exception {
	Pipeline pipeline = new MRPipeline(MapsTest.class);
    String shakesInputPath = FileHelper.createTempCopyOf("shakes.txt");
    PCollection<String> shakespeare = pipeline.readTextFile(shakesInputPath);
    Iterable<Pair<String, Map<String, Long>>> output = shakespeare.parallelDo(
      new DoFn<String, Pair<String, Map<String, Long>>>() {
	    @Override
	    public void process(String input,
		    Emitter<Pair<String, Map<String, Long>>> emitter) {
		  String last = null;
		  for (String word : input.toLowerCase().split("\\W+")) {
		    if (!word.isEmpty()) {
			  String firstChar = word.substring(0, 1);
		      if (last != null) {
		    	Map<String, Long> cc = ImmutableMap.of(firstChar, 1L);
			    emitter.emit(Pair.of(last, cc));
		      }
		      last = firstChar;
		    }
		  }
	    }
      }, typeFamily.tableOf(typeFamily.strings(), typeFamily.maps(typeFamily.longs())))
      .groupByKey()
      .combineValues(new CombineFn<String, Map<String, Long>>() {
	    @Override
	    public void process(Pair<String, Iterable<Map<String, Long>>> input,
		    Emitter<Pair<String, Map<String, Long>>> emitter) {
		  Map<String, Long> agg = Maps.newHashMap();
		  for (Map<String, Long> in : input.second()) {
		    for (Map.Entry<String, Long> e : in.entrySet()) {
			  if (!agg.containsKey(e.getKey())) {
			    agg.put(e.getKey(), e.getValue());
			  } else {
			    agg.put(e.getKey(), e.getValue() + agg.get(e.getKey()));
			  }
		    }
		  }
		  emitter.emit(Pair.of(input.first(), agg));
	    }
	  }).materialize();
    boolean passed = false;
    for (Pair<String, Map<String, Long>> v : output) {
      if (v.first() == "k" && v.second().get("n") == 8L) {
    	passed = true;
    	break;
      }
    }
    pipeline.done();
  }
}
