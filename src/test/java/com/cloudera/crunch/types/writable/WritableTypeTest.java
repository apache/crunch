package com.cloudera.crunch.types.writable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class WritableTypeTest {

  @Test
  public void testGetDetachedValue_AlreadyMappedWritable() {
    WritableType<String, Text> stringType = Writables.strings();
    String value = "test";
    assertSame(value, stringType.getDetachedValue(value));
  }

  @Test
  public void testGetDetachedValue_CustomWritable() {
    WritableType<Text, Text> textWritableType = Writables.writables(Text.class);
    Text value = new Text("test");

    Text detachedValue = textWritableType.getDetachedValue(value);
    assertEquals(value, detachedValue);
    assertNotSame(value, detachedValue);
  }

}
