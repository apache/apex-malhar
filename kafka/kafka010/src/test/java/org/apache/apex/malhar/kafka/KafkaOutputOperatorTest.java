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

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;

public class KafkaOutputOperatorTest extends AbstractKafkaOutputOperatorTest
{
  @BeforeClass
  public static void beforeClass()
  {
    try {
      kafkaserver = new EmbeddedKafka();
      kafkaserver.start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public static void afterClass()
  {
    try {
      kafkaserver.stop();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public AbstractKafkaExactlyOnceOutputOperator<AbstractKafkaOutputOperatorTest.Person> createExaactlyOnceOutputOperator()
  {
    return new KafkaSinglePortExactlyOnceOutputOperator<Person>();
  }

  @Override
  public AbstractKafkaInputOperator createKafkaInputOperator(DAG dag, DefaultInputPort inputPort)
  {
    KafkaSinglePortInputOperator node = dag.addOperator("Kafka input", KafkaSinglePortInputOperator.class);
    // Connect ports
    dag.addStream("Kafka message", node.outputPort, inputPort);

    return node;
  }
}
