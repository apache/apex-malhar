/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.kafka;

import com.datatorrent.common.util.DTThrowable;
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
      try {
        byte[] bytes;
        ByteBuffer buffer = message.payload();
        bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
      }
      catch (Exception ex) {
        DTThrowable.rethrow(ex);
      }
      return null;
    }

  }
