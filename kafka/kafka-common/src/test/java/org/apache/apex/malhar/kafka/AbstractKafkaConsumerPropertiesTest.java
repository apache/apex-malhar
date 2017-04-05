/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.kafka;

import java.text.ParseException;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.kafka.common.KafkaException;

import com.datatorrent.api.Context;

public abstract class  AbstractKafkaConsumerPropertiesTest
{

  public abstract AbstractKafkaInputOperator createKafkaInputOperator();

  public abstract String expectedException();

  AbstractKafkaInputOperator kafkaInput = createKafkaInputOperator();
  @Rule
  public Watcher watcher = new Watcher();

  public class Watcher extends TestWatcher
  {
    Context.OperatorContext context;

    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      kafkaInput.setClusters("localhost:8087");
      kafkaInput.setInitialPartitionCount(1);
      kafkaInput.setTopics("apexTest");
      kafkaInput.setInitialOffset(AbstractKafkaInputOperator.InitialOffset.EARLIEST.name());
      Properties prop = new Properties();
      prop.setProperty("security.protocol", "SASL_PLAINTEXT");
      prop.setProperty("sasl.kerberos.service.name", "kafka");
      kafkaInput.setConsumerProps(prop);
    }

    @Override
    protected void finished(Description description)
    {
      super.finished(description);
    }
  }

  @Test
  public void TestConsumerProperties() throws ParseException
  {
    //Added test on this check to ensure consumer properties are set and not reset between.
    if (null != kafkaInput.getConsumerProps().get("security.protocol")) {
      try {
        kafkaInput.definePartitions(null, null);
      } catch (KafkaException e) {
        //Ensures the  properties of the consumer are set/not reset.
        Assert.assertEquals(
            expectedException(), e.getCause().getMessage());
      }
    }
  }
}
