/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.contrib.kafka.AbstractKafkaSinglePortInputOperator;
import java.nio.ByteBuffer;
import kafka.message.Message;

  public class KafkaSinglePortByteArrayInputOperator extends AbstractKafkaSinglePortInputOperator<byte[]>
  {

    /**
     * Implement abstract method of AbstractKafkaSinglePortInputOperator
     *
     * @param message
     * @return byte Array
     */
    @Override
    public byte[] getTuple(Message message)
    {
      byte[] bytes = null;
      try {
        ByteBuffer buffer = message.payload();
        bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
      }
      catch (Exception ex) {
        return bytes;
      }
      return bytes;
    }

  }
