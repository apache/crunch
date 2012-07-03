package com.cloudera.crunch.types.writable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.util.List;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.cloudera.crunch.Pair;
import com.cloudera.crunch.types.PGroupedTableType;
import com.google.common.collect.Lists;

public class WritableGroupedTableTypeTest {

  @Test
  public void testGetDetachedValue() {
    Integer integerValue = 42;
    Text textValue = new Text("forty-two");
    Iterable<Text> inputTextIterable = Lists.newArrayList(textValue);
    Pair<Integer, Iterable<Text>> pair = Pair.of(integerValue, inputTextIterable);

    PGroupedTableType<Integer, Text> groupedTableType = Writables.tableOf(Writables.ints(), Writables.writables(Text.class))
        .getGroupedTableType();
    
    Pair<Integer, Iterable<Text>> detachedPair = groupedTableType.getDetachedValue(pair);
    
    assertSame(integerValue, detachedPair.first());
    List<Text> textList = Lists.newArrayList(detachedPair.second());
    assertEquals(inputTextIterable, textList);
    assertNotSame(textValue, textList.get(0));
    
  }

}
