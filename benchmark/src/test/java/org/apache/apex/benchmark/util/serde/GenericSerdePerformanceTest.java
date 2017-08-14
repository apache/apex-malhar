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
package org.apache.apex.benchmark.util.serde;

import java.util.Random;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.utils.serde.GenericSerde;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.utils.serde.SerializationBuffer;
import org.apache.apex.malhar.lib.utils.serde.StringSerde;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.commons.lang3.tuple.ImmutablePair;

import com.esotericsoftware.kryo.Kryo;

public class GenericSerdePerformanceTest
{
  private static final transient Logger logger = LoggerFactory.getLogger(GenericSerdePerformanceTest.class);
  private SerializationBuffer buffer = SerializationBuffer.READ_BUFFER;
  private Random random = new Random();
  private int serdeDataSize = 1000000;

  @Test
  public void testCompareSerdeForString()
  {
    long beginTime = System.currentTimeMillis();
    testSerdeForString(new GenericSerde<String>(String.class));
    long genericSerdeCost = System.currentTimeMillis() - beginTime;
    logger.info("Generic Serde cost for String: {}", genericSerdeCost);

    beginTime = System.currentTimeMillis();
    testSerdeForString(new StringSerde());
    long stringSerdeCost = System.currentTimeMillis() - beginTime;
    logger.info("String Serde cost for String: {}", stringSerdeCost);

    beginTime = System.currentTimeMillis();
    Kryo kryo = new Kryo();
    for (int i = 0; i < serdeDataSize; ++i) {
      kryo.writeObject(buffer, "" + random.nextInt(1000));
      buffer.toSlice();
    }
    buffer.release();
    long kryoSerdeCost = System.currentTimeMillis() - beginTime;
    logger.info("Kryo Serde cost for String: {}", kryoSerdeCost);
  }

  protected void testSerdeForString(Serde<String> serde)
  {
    for (int i = 0; i < serdeDataSize; ++i) {
      serde.serialize("" + random.nextInt(1000), buffer);
      buffer.toSlice();
    }
    buffer.release();
  }

  @Test
  public void testCompareSerdeForRealCase()
  {
    long beginTime = System.currentTimeMillis();
    GenericSerde<ImmutablePair> serde = new GenericSerde<ImmutablePair>();
    for (int i = 0; i < serdeDataSize; ++i) {
      serde.serialize(generatePair(beginTime), buffer);
      buffer.toSlice();
    }
    buffer.release();
    long genericSerdeCost = System.currentTimeMillis() - beginTime;
    logger.info("Generic Serde cost for ImmutablePair: {}", genericSerdeCost);

    beginTime = System.currentTimeMillis();
    Kryo kryo = new Kryo();
    for (int i = 0; i < serdeDataSize; ++i) {
      kryo.writeObject(buffer, generatePair(beginTime));
      buffer.toSlice();
    }
    buffer.release();
    long kryoSerdeCost = System.currentTimeMillis() - beginTime;
    logger.info("Kryo Serde cost for ImmutablePair without class info: {}", kryoSerdeCost);

    beginTime = System.currentTimeMillis();
    Kryo kryo1 = new Kryo();
    for (int i = 0; i < serdeDataSize; ++i) {
      kryo1.writeClassAndObject(buffer, generatePair(beginTime));
      buffer.toSlice();
    }
    buffer.release();
    long kryoSerdeCost2 = System.currentTimeMillis() - beginTime;
    logger.info("Kryo Serde cost for ImmutablePair with class info: {}", kryoSerdeCost2);
  }

  protected ImmutablePair generatePair(long now)
  {
    return new ImmutablePair(new Window.TimeWindow(now + random.nextInt(100),
        random.nextInt(100)), "" + random.nextInt(1000));
  }
}
