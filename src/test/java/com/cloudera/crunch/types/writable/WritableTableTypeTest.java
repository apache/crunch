package com.cloudera.crunch.types.writable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.cloudera.crunch.Pair;

public class WritableTableTypeTest {

  @Test
  public void testGetDetachedValue() {
    Integer integerValue = 42;
    Text textValue = new Text("forty-two");
    Pair<Integer, Text> pair = Pair.of(integerValue, textValue);

    WritableTableType<Integer, Text> tableType = Writables.tableOf(Writables.ints(),
        Writables.writables(Text.class));

    Pair<Integer, Text> detachedPair = tableType.getDetachedValue(pair);

    assertSame(integerValue, detachedPair.first());
    assertEquals(textValue, detachedPair.second());
    assertNotSame(textValue, detachedPair.second());
  }

}
