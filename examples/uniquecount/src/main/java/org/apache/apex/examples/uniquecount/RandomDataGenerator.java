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
package org.apache.apex.examples.uniquecount;

import java.util.HashMap;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.util.KeyValPair;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

/**
 * Generate random Key value pairs.
 * key is string and value is int, it emits the pair as KeyValPair on outPort,
 *
 * @since 1.0.2
 */
public class RandomDataGenerator implements InputOperator
{
  public final transient DefaultOutputPort<KeyValPair<String, Object>> outPort = new DefaultOutputPort<KeyValPair<String, Object>>();
  private HashMap<String, Integer> dataInfo;
  private final transient Logger LOG = LoggerFactory.getLogger(RandomDataGenerator.class);
  private int count;
  private int sleepMs = 10;
  private int keyRange = 100;
  private int valueRange = 100;
  private long tupleBlast = 10000;
  private Random random;

  public RandomDataGenerator()
  {
    random = new Random();
  }

  @Override
  public void emitTuples()
  {
    for (int i = 0; i < tupleBlast; i++) {
      String key = String.valueOf(random.nextInt(keyRange));
      int val = random.nextInt(valueRange);
      outPort.emit(new KeyValPair<String, Object>(key, val));
    }
    try {
      Thread.sleep(sleepMs);
    } catch (Exception ex) {
      LOG.error(ex.getMessage());
    }
    count++;
  }

  public int getSleepMs()
  {
    return sleepMs;
  }

  public void setSleepMs(int sleepMs)
  {
    this.sleepMs = sleepMs;
  }

  public long getTupleBlast()
  {
    return tupleBlast;
  }

  public void setTupleBlast(long tupleBlast)
  {
    this.tupleBlast = tupleBlast;
  }

  @Override
  public void beginWindow(long l)
  {

  }

  @Override
  public void endWindow()
  {
    LOG.debug("emitTuples called  " + count + " times in this window");
    count = 0;
  }

  @Override
  public void setup(Context.OperatorContext operatorContext)
  {

  }

  @Override
  public void teardown()
  {

  }
}
