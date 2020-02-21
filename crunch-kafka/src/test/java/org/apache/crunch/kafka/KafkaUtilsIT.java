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
package org.apache.crunch.kafka;

import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.Properties;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;


public class KafkaUtilsIT {

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void startup() throws Exception {
    ClusterTest.startTest();
  }

  @AfterClass
  public static void shutdown() throws Exception {
    ClusterTest.endTest();
  }

  @Test
  public void getKafkaProperties() {
    Configuration config = new Configuration(false);
    String propertyKey = "fake.kafka.property";
    String propertyValue = testName.getMethodName();
    config.set(propertyKey, propertyValue);

    Properties props = KafkaUtils.getKafkaConnectionProperties(config);
    assertThat(props.get(propertyKey), is((Object) propertyValue));
  }

  @Test
  public void addKafkaProperties() {
    String propertyKey = "fake.kafka.property";
    String propertyValue = testName.getMethodName();

    Properties props = new Properties();
    props.setProperty(propertyKey, propertyValue);

    Configuration config = new Configuration(false);

    KafkaUtils.addKafkaConnectionProperties(props, config);
    assertThat(config.get(propertyKey), is(propertyValue));
  }


}
