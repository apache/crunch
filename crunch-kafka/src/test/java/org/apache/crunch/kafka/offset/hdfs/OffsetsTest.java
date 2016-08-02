/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.crunch.kafka.offset.hdfs;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.api.OffsetRequest;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class OffsetsTest {

  @Rule
  public TestName testName = new TestName();

  private static ObjectMapper mapper;

  @BeforeClass
  public static void setup() {
    mapper = new ObjectMapper();
  }

  @Test(expected = IllegalArgumentException.class)
  public void buildOffsetNullOffsets() {
    Offsets.Builder.newBuilder().setOffsets(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void buildInvalidAsOfTime() {
    Offsets.Builder.newBuilder().setAsOfTime(-1);
  }

  @Test(expected = IllegalStateException.class)
  public void buildNoAsOfTime() {
    Offsets.Builder.newBuilder().build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void buildPartitionNullTopic() {
    Offsets.PartitionOffset.Builder.newBuilder().setTopic(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void buildPartitionEmptyTopic() {
    Offsets.PartitionOffset.Builder.newBuilder().setTopic(" ");
  }

  @Test(expected = IllegalArgumentException.class)
  public void buildPartitionInvalidPartition() {
    Offsets.PartitionOffset.Builder.newBuilder().setPartition(-1);
  }

  @Test(expected = IllegalStateException.class)
  public void buildPartitionNoTopicSet() {
    Offsets.PartitionOffset.Builder.newBuilder().setPartition(10).setOffset(10L).build();
  }

  @Test(expected = IllegalStateException.class)
  public void buildPartitionNoPartitionSet() {
    Offsets.PartitionOffset.Builder.newBuilder().setTopic(testName.getMethodName()).setOffset(10L).build();
  }

  @Test
  public void buildPartitionOffset() {
    Offsets.PartitionOffset partitionOffset = Offsets.PartitionOffset.Builder.newBuilder()
        .setTopic(testName.getMethodName()).setOffset(10L).setPartition(1).build();

    assertThat(partitionOffset.getOffset(), is(10L));
    assertThat(partitionOffset.getPartition(), is(1));
    assertThat(partitionOffset.getTopic(), is(testName.getMethodName()));
  }

  @Test
  public void buildPartitionOffsetNoOffsetSet() {
    Offsets.PartitionOffset partitionOffset = Offsets.PartitionOffset.Builder.newBuilder()
        .setTopic(testName.getMethodName()).setPartition(1).build();

    assertThat(partitionOffset.getOffset(), is(OffsetRequest.EarliestTime()));
    assertThat(partitionOffset.getPartition(), is(1));
    assertThat(partitionOffset.getTopic(), is(testName.getMethodName()));
  }

  @Test
  public void partitionOffsetSame() {
    Offsets.PartitionOffset partitionOffset = Offsets.PartitionOffset.Builder.newBuilder()
        .setTopic(testName.getMethodName()).setOffset(10L).setPartition(1).build();

    assertThat(partitionOffset.equals(partitionOffset), is(true));
    assertThat(partitionOffset.compareTo(partitionOffset), is(0));
  }

  @Test
  public void partitionOffsetEqual() {
    Offsets.PartitionOffset partitionOffset1 = Offsets.PartitionOffset.Builder.newBuilder()
        .setTopic(testName.getMethodName()).setOffset(10L).setPartition(1).build();

    Offsets.PartitionOffset partitionOffset2 = Offsets.PartitionOffset.Builder.newBuilder()
        .setTopic(testName.getMethodName()).setOffset(10L).setPartition(1).build();

    assertThat(partitionOffset1.equals(partitionOffset2), is(true));
    assertThat(partitionOffset1.compareTo(partitionOffset2), is(0));
  }

  @Test
  public void partitionOffsetNotEqualDiffTopic() {
    Offsets.PartitionOffset partitionOffset1 = Offsets.PartitionOffset.Builder.newBuilder()
        .setTopic("abc").setOffset(10L).setPartition(1).build();

    Offsets.PartitionOffset partitionOffset2 = Offsets.PartitionOffset.Builder.newBuilder()
        .setTopic(testName.getMethodName()).setOffset(10L).setPartition(1).build();

    assertThat(partitionOffset1.equals(partitionOffset2), is(false));
    assertThat(partitionOffset1.compareTo(partitionOffset2), is(lessThan(0)));
  }

  @Test
  public void partitionOffsetNotEqualDiffPartition() {
    Offsets.PartitionOffset partitionOffset1 = Offsets.PartitionOffset.Builder.newBuilder()
        .setTopic(testName.getMethodName()).setOffset(10L).setPartition(0).build();

    Offsets.PartitionOffset partitionOffset2 = Offsets.PartitionOffset.Builder.newBuilder()
        .setTopic(testName.getMethodName()).setOffset(10L).setPartition(1).build();

    assertThat(partitionOffset1.equals(partitionOffset2), is(false));
    assertThat(partitionOffset1.compareTo(partitionOffset2), is(lessThan(0)));
  }

  @Test
  public void partitionOffsetNotEqualDiffOffset() {
    Offsets.PartitionOffset partitionOffset1 = Offsets.PartitionOffset.Builder.newBuilder()
        .setTopic(testName.getMethodName()).setOffset(9L).setPartition(1).build();

    Offsets.PartitionOffset partitionOffset2 = Offsets.PartitionOffset.Builder.newBuilder()
        .setTopic(testName.getMethodName()).setOffset(10L).setPartition(1).build();

    assertThat(partitionOffset1.equals(partitionOffset2), is(false));
    assertThat(partitionOffset1.compareTo(partitionOffset2), is(lessThan(0)));
  }


  @Test
  public void partitionOffsetNotEqualDiffGreaterTopic() {
    Offsets.PartitionOffset partitionOffset1 = Offsets.PartitionOffset.Builder.newBuilder()
        .setTopic(testName.getMethodName()).setOffset(10L).setPartition(1).build();

    Offsets.PartitionOffset partitionOffset2 = Offsets.PartitionOffset.Builder.newBuilder()
        .setTopic("abc").setOffset(10L).setPartition(1).build();

    assertThat(partitionOffset1.equals(partitionOffset2), is(false));
    assertThat(partitionOffset1.compareTo(partitionOffset2), is(greaterThan(0)));
  }

  @Test
  public void partitionOffsetNotEqualDiffGreaterPartition() {
    Offsets.PartitionOffset partitionOffset1 = Offsets.PartitionOffset.Builder.newBuilder()
        .setTopic(testName.getMethodName()).setOffset(10L).setPartition(2).build();

    Offsets.PartitionOffset partitionOffset2 = Offsets.PartitionOffset.Builder.newBuilder()
        .setTopic(testName.getMethodName()).setOffset(10L).setPartition(1).build();

    assertThat(partitionOffset1.equals(partitionOffset2), is(false));
    assertThat(partitionOffset1.compareTo(partitionOffset2), is(greaterThan(0)));
  }

  @Test
  public void partitionOffsetNotEqualDiffGreaterOffset() {
    Offsets.PartitionOffset partitionOffset1 = Offsets.PartitionOffset.Builder.newBuilder()
        .setTopic(testName.getMethodName()).setOffset(12L).setPartition(1).build();

    Offsets.PartitionOffset partitionOffset2 = Offsets.PartitionOffset.Builder.newBuilder()
        .setTopic(testName.getMethodName()).setOffset(10L).setPartition(1).build();

    assertThat(partitionOffset1.equals(partitionOffset2), is(false));
    assertThat(partitionOffset1.compareTo(partitionOffset2), is(greaterThan(0)));
  }

  @Test
  public void jsonSerializationPartitionOffset() throws IOException {
    Offsets.PartitionOffset partitionOffset = Offsets.PartitionOffset.Builder.newBuilder()
        .setTopic(testName.getMethodName()).setOffset(12L).setPartition(1).build();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    mapper.writeValue(baos, partitionOffset);

    Offsets.PartitionOffset readOffset = mapper.readValue(baos.toByteArray(), Offsets.PartitionOffset.class);

    assertThat(readOffset, is(partitionOffset));
  }


  @Test
  public void buildOffsetsNoOffsets() {
    Offsets offsets = Offsets.Builder.newBuilder().setAsOfTime(10).build();
    assertThat(offsets.getAsOfTime(), is(10L));
    assertThat(offsets.getOffsets(), is(Collections.<Offsets.PartitionOffset>emptyList()));
  }

  @Test
  public void buildOffsetsSortOffsets() {
    int partition = 0;
    long offset = 10L;

    List<Offsets.PartitionOffset> reversedOffsets = new LinkedList<>();
    for(int i = 0; i < 9; i++){
      reversedOffsets.add(Offsets.PartitionOffset.Builder.newBuilder().setTopic("topic" + (9 - i))
          .setPartition(partition).setOffset(offset).build());
    }

    Offsets offsets = Offsets.Builder.newBuilder().setAsOfTime(10).setOffsets(reversedOffsets).build();
    assertThat(offsets.getAsOfTime(), is(10L));

    List<Offsets.PartitionOffset> returnedOffsets = offsets.getOffsets();
    int count = 1;

    //iterate in the expected order
    for (Offsets.PartitionOffset o : returnedOffsets) {
      assertThat(o.getTopic(), is("topic" + count));
      assertThat(o.getPartition(), is(partition));
      assertThat(o.getOffset(), is(offset));
      count++;
    }
    assertThat(count, is(10));
  }

  @Test
  public void offsetsSame() {
    int partition = 0;
    long offset = 10L;

    List<Offsets.PartitionOffset> reversedOffsets = new LinkedList<>();
    for(int i = 0; i < 9; i++){
      reversedOffsets.add(Offsets.PartitionOffset.Builder.newBuilder().setTopic("topic" + (9 - i))
          .setPartition(partition).setOffset(offset).build());
    }

    Offsets offsets = Offsets.Builder.newBuilder().setAsOfTime(10).setOffsets(reversedOffsets).build();
    assertThat(offsets.getAsOfTime(), is(10L));

    assertThat(offsets.equals(offsets), is(true));
  }

  @Test
  public void offsetsEqual() {
    int partition = 0;
    long offset = 10L;

    List<Offsets.PartitionOffset> reversedOffsets = new LinkedList<>();
    for(int i = 0; i < 9; i++){
      reversedOffsets.add(Offsets.PartitionOffset.Builder.newBuilder().setTopic("topic" + (9 - i))
          .setPartition(partition).setOffset(offset).build());
    }


    Offsets offsets = Offsets.Builder.newBuilder().setAsOfTime(10).setOffsets(reversedOffsets).build();
    assertThat(offsets.getAsOfTime(), is(10L));

    Offsets offsets2 = Offsets.Builder.newBuilder().setAsOfTime(10).setOffsets(reversedOffsets).build();
    assertThat(offsets.getAsOfTime(), is(10L));

    assertThat(offsets.equals(offsets2), is(true));
  }

  @Test
  public void offsetsDiffAsOfTime() {
    int partition = 0;
    long offset = 10L;

    List<Offsets.PartitionOffset> reversedOffsets = new LinkedList<>();
    for(int i = 0; i < 9; i++){
      reversedOffsets.add(Offsets.PartitionOffset.Builder.newBuilder().setTopic("topic" + (9 - i))
          .setPartition(partition).setOffset(offset).build());
    }

    Offsets offsets = Offsets.Builder.newBuilder().setAsOfTime(10).setOffsets(reversedOffsets).build();
    assertThat(offsets.getAsOfTime(), is(10L));

    Offsets offsets2 = Offsets.Builder.newBuilder().setAsOfTime(11).setOffsets(reversedOffsets).build();
    assertThat(offsets.getAsOfTime(), is(10L));

    assertThat(offsets.equals(offsets2), is(false));
  }

  @Test
  public void offsetsDiffOffsets() {
    int partition = 0;
    long offset = 10L;

    List<Offsets.PartitionOffset> reversedOffsets = new LinkedList<>();
    for(int i = 0; i < 9; i++){
      reversedOffsets.add(Offsets.PartitionOffset.Builder.newBuilder().setTopic("topic" + (9 - i))
          .setPartition(partition).setOffset(offset).build());
    }

    List<Offsets.PartitionOffset> secondOffsets = new LinkedList<>();
    for(int i = 0; i < 5; i++){
      secondOffsets.add(Offsets.PartitionOffset.Builder.newBuilder().setTopic("topic" + (9 - i))
          .setPartition(partition).setOffset(offset).build());
    }


    Offsets offsets = Offsets.Builder.newBuilder().setAsOfTime(10).setOffsets(reversedOffsets).build();
    assertThat(offsets.getAsOfTime(), is(10L));

    Offsets offsets2 = Offsets.Builder.newBuilder().setAsOfTime(10).setOffsets(secondOffsets).build();
    assertThat(offsets.getAsOfTime(), is(10L));

    assertThat(offsets.equals(offsets2), is(false));
  }

  @Test(expected = IllegalStateException.class)
  public void offsetsDuplicates() {
    int partition = 0;
    long offset = 10L;

    List<Offsets.PartitionOffset> reversedOffsets = new LinkedList<>();
    for(int i = 0; i < 9; i++){
      reversedOffsets.add(Offsets.PartitionOffset.Builder.newBuilder().setTopic("topic" + (9 - i))
          .setPartition(partition).setOffset(offset).build());
    }

    reversedOffsets.add(Offsets.PartitionOffset.Builder.newBuilder().setTopic("topic9").setPartition(0).build());

    Offsets offsets = Offsets.Builder.newBuilder().setAsOfTime(10).setOffsets(reversedOffsets).build();
  }

  @Test
  public void offsetsDiffListInstances() {
    int partition = 0;
    long offset = 10L;

    List<Offsets.PartitionOffset> reversedOffsets = new LinkedList<>();
    for(int i = 0; i < 9; i++){
      reversedOffsets.add(Offsets.PartitionOffset.Builder.newBuilder().setTopic("topic" + (9 - i))
          .setPartition(partition).setOffset(offset).build());
    }

    List<Offsets.PartitionOffset> secondOffsets = new LinkedList<>();
    for(int i = 0; i < 9; i++){
      secondOffsets.add(Offsets.PartitionOffset.Builder.newBuilder().setTopic("topic" + (9 - i))
          .setPartition(partition).setOffset(offset).build());
    }


    Offsets offsets = Offsets.Builder.newBuilder().setAsOfTime(10).setOffsets(reversedOffsets).build();
    assertThat(offsets.getAsOfTime(), is(10L));

    Offsets offsets2 = Offsets.Builder.newBuilder().setAsOfTime(10).setOffsets(secondOffsets).build();
    assertThat(offsets.getAsOfTime(), is(10L));

    assertThat(offsets.equals(offsets2), is(true));
  }

  @Test
  public void offsetsEqualEmptyOffsets() {
    int partition = 0;
    long offset = 10L;

    List<Offsets.PartitionOffset> reversedOffsets = new LinkedList<>();

    List<Offsets.PartitionOffset> secondOffsets = new LinkedList<>();


    Offsets offsets = Offsets.Builder.newBuilder().setAsOfTime(10).setOffsets(reversedOffsets).build();
    assertThat(offsets.getAsOfTime(), is(10L));

    Offsets offsets2 = Offsets.Builder.newBuilder().setAsOfTime(10).setOffsets(secondOffsets).build();
    assertThat(offsets.getAsOfTime(), is(10L));

    assertThat(offsets.equals(offsets2), is(true));
  }

  @Test
  public void jsonSerializationOffsets() throws IOException {
    int partition = 0;
    long offset = 10L;

    List<Offsets.PartitionOffset> partitionOffsets = new LinkedList<>();
    for(int i = 0; i < 100; i++){
      partitionOffsets.add(Offsets.PartitionOffset.Builder.newBuilder().setTopic("topic" + i)
          .setPartition(partition).setOffset(offset).build());
    }


    Offsets offsets = Offsets.Builder.newBuilder().setAsOfTime(10).setOffsets(partitionOffsets).build();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    mapper.writeValue(baos, offsets);

    Offsets readOffsets = mapper.readValue(baos.toByteArray(), Offsets.class);

    assertThat(readOffsets, is(offsets));
  }

  @Test
  public void jsonSerializationOffsetsEmpty() throws IOException {
    int partition = 0;
    long offset = 10L;


    Offsets offsets = Offsets.Builder.newBuilder().setAsOfTime(10)
        .setOffsets(Collections.<Offsets.PartitionOffset>emptyList()).build();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    mapper.writeValue(baos, offsets);

    Offsets readOffsets = mapper.readValue(baos.toByteArray(), Offsets.class);

    assertThat(readOffsets, is(offsets));
  }
}
