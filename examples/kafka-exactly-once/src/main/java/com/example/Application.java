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
package com.example;

import java.util.Properties;

import org.apache.apex.malhar.kafka.KafkaSinglePortExactlyOnceOutputOperator;
import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.apex.malhar.kafka.KafkaSinglePortOutputOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * Kafka exactly-once example (Kafka 0.9 API)
 *
 * This application verifies exactly-once semantics by writing a defined sequence of input data to two Kafka
 * output operators -- one that guarantees those semantics (using KafkaSingleExactlyOnceOutputOperator) and one that does not,
 * each writing to a different topic. It deliberately causes the intermediate pass-through
 * operator to fail causing it to be restarted and some tuples to be reprocessed.
 * Then a KafkaInputOperator reads tuples from both topics to verify that the former topic has no duplicates
 * but the latter does and writes a single line to a HDFS file with the verification results
 * of the following form:
 *
 * Duplicates: exactly-once: 0, at-least-once: 5
 *
 * Operators:
 *
 * **BatchSequenceGenerator:**
 * Generates a sequence of numbers starting going from 1 to maxTuplesTotal which can be set in the properties.
 *
 * **PassthroughFailOperator:**
 * This operator kills itself after a defined number of processed tuples by intentionally throwing an exception.
 * STRAM will redeploy the operator on a new container. The exception only needs to be thrown once, so a file is
 * written to HDFS just before throwing the exception and its presence is checked after restart to determine
 * if the exception was already thrown.
 *
 * **KafkaSinglePortExactlyOnceOutputOperator:**
 * Topic, bootstrap.servers, serializer and deserializer are set in properties.xml.
 * The topic names should not be changed for this application.
 *
 * **ValidationToFile:**
 * Puts values of input into list depending on topic. If value of maxTuplesTotal is reached it will calculate duplicates
 * and write validation output to HDFS. (output line will be printed in container dt.log as well).
 */

@ApplicationAnnotation(name = "KafkaExactlyOnce")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    BatchSequenceGenerator sequenceGenerator = dag.addOperator("sequenceGenerator", BatchSequenceGenerator.class);
    PassthroughFailOperator passthroughFailOperator = dag.addOperator("passthrough", PassthroughFailOperator.class);
    KafkaSinglePortExactlyOnceOutputOperator<String> kafkaExactlyOnceOutputOperator =
        dag.addOperator("kafkaExactlyOnceOutputOperator", KafkaSinglePortExactlyOnceOutputOperator.class);
    KafkaSinglePortOutputOperator kafkaOutputOperator =
        dag.addOperator("kafkaOutputOperator", KafkaSinglePortOutputOperator.class);

    dag.addStream("sequenceToPassthrough", sequenceGenerator.out, passthroughFailOperator.input);
    dag.addStream("linesToKafka", passthroughFailOperator.output, kafkaOutputOperator.inputPort,
        kafkaExactlyOnceOutputOperator.inputPort);

    KafkaSinglePortInputOperator kafkaInputTopicExactly = dag.addOperator("kafkaTopicExactly", KafkaSinglePortInputOperator.class);
    kafkaInputTopicExactly.setInitialOffset(KafkaTopicMessageInputOperator.InitialOffset.EARLIEST.name());

    KafkaSinglePortInputOperator kafkaInputTopicAtLeast = dag.addOperator("kafkaTopicAtLeast", KafkaSinglePortInputOperator.class);
    kafkaInputTopicAtLeast.setInitialOffset(KafkaTopicMessageInputOperator.InitialOffset.EARLIEST.name());

    ValidationToFile validationToFile = dag.addOperator("validationToFile", ValidationToFile.class);

    dag.addStream("messagesFromExactly", kafkaInputTopicExactly.outputPort, validationToFile.topicExactlyInput);
    dag.addStream("messagesFromAtLeast", kafkaInputTopicAtLeast.outputPort, validationToFile.topicAtLeastInput);

  }
}
