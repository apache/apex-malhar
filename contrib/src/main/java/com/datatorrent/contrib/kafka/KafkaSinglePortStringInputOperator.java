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

import com.datatorrent.common.util.DTThrowable;
import kafka.message.Message;

import java.nio.ByteBuffer;

/**
 * Kafka input adapter operator with a single output port, which consumes String data from the Kafka message bus.
 * <p></p>
 * @deprecated Please use KafkaSinglePortByteArrayInputOperator and ByteArrayToStringConverter as Thread Local instead of KafkaSinglePortStringInputOperator.
 * @displayName Kafka Single Port String Input
 * @category Messaging
 * @tags input operator, string
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
    try {
      String data;
      ByteBuffer buffer = message.payload();
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      data = new String(bytes);
      return data;
      //logger.debug("Consuming {}", data);
    }
    catch (Exception ex) {
      DTThrowable.rethrow(ex);
    }
    return null;
  }

}
