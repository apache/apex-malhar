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

import java.util.Properties;

/**
 * Kafka output operator with exactly once processing semantics.
 * <br>
 *
 * <p>
 * <b>Requirements</b>
 * <li>In the Kafka message, only Value will be available for users</li>
 * <li>Users need to provide Value deserializers for Kafka message as it is used during recovery</li>
 * <li>Value type should have well defined Equals & HashCodes,
 * as during messages are stored in HashMaps for comparison.</li>
 * <p>
 * <b>Recovery handling</b>
 * <li> Offsets of the Kafka partitions are stored in the WindowDataManager at the endWindow</li>
 * <li> During recovery,
 * <ul>
 * <li>Partially written Streaming Window before the crash is constructed. ( Explained below ) </li>
 * <li>Tuples from the completed Streaming Window's are skipped </li>
 * <li>Tuples coming for the partially written Streaming Window are skipped.
 * (No assumption is made on the order and the uniqueness of the tuples) </li>
 * </ul>
 * </li>
 * </p>
 *
 * <p>
 * <b>Partial Window Construction</b>
 * <li> Operator uses the Key in the Kafka message, which is not available for use by the operator users.</li>
 * <li> Key is used to uniquely identify the message written by the particular instance of this operator.</li>
 * This allows multiple writers to same Kafka partitions. Format of the key is "APPLICATTION_ID#OPERATOR_ID".
 * <li>During recovery Kafka partitions are read between the latest offset and the last written offsets.</li>
 * <li>All the tuples written by the particular instance is kept in the Map</li>
 * </p>
 *
 * <p>
 * <b>Limitations</b>
 * <li> Key in the Kafka message is reserved for Operator's use </li>
 * <li> During recovery, operator needs to read tuples between 2 offsets,
 * if there are lot of data to be read, Operator may
 * appear to be blocked to the Stram and can kill the operator. </li>
 * </p>
 *
 * @displayName Kafka Single Port Exactly Once Output(0.9.0)
 * @category Messaging
 * @tags output operator
 * @since 3.5.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class KafkaSinglePortExactlyOnceOutputOperator<T> extends AbstractKafkaExactlyOnceOutputOperator
{

  @Override
  public AbstractKafkaConsumer createConsumer(Properties prop)
  {
    return new KafkaConsumer010<String, T>(prop);
  }
}
