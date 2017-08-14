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
package org.apache.apex.malhar.contrib.kafka;

import java.util.Map;

/**
 * An offset manager interface used by  {@link AbstractKafkaInputOperator} to define the customized initial offsets and periodically update the current offsets of all the operators
 * <br>
 * <br>
 * Ex. you could write offset to hdfs and load it back when restart the application
 *
 * @since 2.0.0
 */
public interface OffsetManager
{
//

  /**
   *
   * Load initial offsets for all kafka partition
   * <br>
   * The method is called at the first attempt of creating partitions and the return value is used as initial offset for simple consumer
   *
   * @return Map of Kafka KafkaPartition as key and long offset as value
   */
  public Map<KafkaPartition, Long> loadInitialOffsets();


  /**
   * @param offsetsOfPartitions offsets for specified partitions, it is reported by individual operator instances
   * <br>
   * The method is called every {@link AbstractKafkaInputOperator#getRepartitionCheckInterval()} to update the current offset
   */
  public void updateOffsets(Map<KafkaPartition, Long> offsetsOfPartitions);

}
