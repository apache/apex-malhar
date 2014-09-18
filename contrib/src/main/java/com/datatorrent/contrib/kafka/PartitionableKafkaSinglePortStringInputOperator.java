/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.kafka;

import com.datatorrent.api.annotation.OperatorAnnotation;
import java.nio.ByteBuffer;
import kafka.message.Message;

/**
 * Kafka input adapter operator with a single output port, which consumes String data from the Kafka message bus.
 * <p></p>
 *
 * @displayName Partitionable Kafka Single Port String Input Operator
 * @category messaging
 * @tags input, string
 *
 * @since 0.9.0
 */
@OperatorAnnotation(partitionable = true)
public class PartitionableKafkaSinglePortStringInputOperator extends AbstractPartitionableKafkaSinglePortInputOperator<String>
{
  /**
   * Implement abstract method of AbstractPartitionableKafkaSinglePortInputOperator
   * Just parse the kafka message as a string
   */
  @Override
  public String getTuple(Message message)
  {
    String data = "";
    try {
      ByteBuffer buffer = message.payload();
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      data = new String(bytes);
//      System.out.println(data);
//      logger.debug("Consuming {}", data);
    }
    catch (Exception ex) {
      return data;
    }
    return data;
  }

  @Override
  protected AbstractPartitionableKafkaInputOperator cloneOperator()
  {
    return new PartitionableKafkaSinglePortStringInputOperator();
  }


}
