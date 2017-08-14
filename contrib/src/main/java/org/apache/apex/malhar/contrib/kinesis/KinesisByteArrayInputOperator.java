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
package org.apache.apex.malhar.contrib.kinesis;

import java.nio.ByteBuffer;

import com.amazonaws.services.kinesis.model.Record;

/**
 * Kinesis input adapter which consumes records from kinesis streams and emits in ByteArray form.
 *
 * @category Input
 * @tags Kinesis, input, ByteArray
 *
 * @since 3.8.0
 */

public class KinesisByteArrayInputOperator extends AbstractKinesisInputOperator<byte[]>
{
  /**
   * Implement abstract method of AbstractKinesisInputOperator
   */
  @Override
  public byte[] getTuple(Record record)
  {
    try {
      ByteBuffer bb = record.getData();
      byte[] bytes = new byte[bb.remaining()];
      bb.get(bytes);
      return bytes;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
