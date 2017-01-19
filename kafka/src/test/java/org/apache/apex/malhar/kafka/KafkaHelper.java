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

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class KafkaHelper implements Serializer<KafkaOutputOperatorTest.Person>,
    Deserializer<KafkaOutputOperatorTest.Person>
{
  @Override
  public KafkaOutputOperatorTest.Person deserialize(String s, byte[] bytes)
  {
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    int nameLength = byteBuffer.getInt();
    byte[] name = new byte[nameLength];

    byteBuffer.get(name, 0, nameLength);

    return new KafkaOutputOperatorTest.Person(new String(name), byteBuffer.getInt());
  }

  @Override
  public byte[] serialize(String s, KafkaOutputOperatorTest.Person person)
  {
    byte[] name = person.name.getBytes();

    ByteBuffer byteBuffer = ByteBuffer.allocate(name.length + 4 + 4);

    byteBuffer.putInt(name.length);
    byteBuffer.put(name);
    byteBuffer.putInt(person.age);

    return byteBuffer.array();
  }

  @Override
  public void configure(Map<String, ?> map, boolean b)
  {
  }

  @Override
  public void close()
  {
  }
}
