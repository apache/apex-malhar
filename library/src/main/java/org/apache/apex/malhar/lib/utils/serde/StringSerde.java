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
package org.apache.apex.malhar.lib.utils.serde;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * An implementation of {@link Serde} which serializes and deserializes {@link String}s.
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public class StringSerde implements Serde<String>
{
  @Override
  public void serialize(String string, SerializationBuffer buffer)
  {
    try {
      buffer.writeUTF(string);
    } catch (IOException e) {
      throw new RuntimeException("Not suppose to get this exception");
    }
  }

  @Override
  public String deserialize(byte[] buffer, MutableInt offset, int length)
  {
    int len = (((buffer[offset.intValue()]) & 0xFF) << 8) | ((buffer[1 + offset.intValue()]) & 0xFF);
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(buffer, offset.intValue(), length));
    offset.add(len + 2);
    try {
      return dis.readUTF();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
