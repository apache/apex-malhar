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

import java.nio.ByteBuffer;
import kafka.message.Message;

/**
 * Kafka input adapter operator with a single output port, which consumes String data from the Kafka message bus.
 * <p></p>
 *
 * @displayName Kafka Single Port String Input Operator
 * @category messaging
 * @tags input, string
 *
 * @since 0.3.5
 */
public class KafkaSinglePortStringInputOperator extends AbstractKafkaSinglePortInputOperator<String>
{

  /**
   * Implement abstract method of AbstractActiveMQSinglePortInputOperator
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
      //logger.debug("Consuming {}", data);
    }
    catch (Exception ex) {
      return data;
    }
    return data;
  }

}
