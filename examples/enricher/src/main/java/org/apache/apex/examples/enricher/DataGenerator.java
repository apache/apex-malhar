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

package org.apache.apex.examples.enricher;

import java.util.Random;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 * Generates Subscriber Data:
 *    A Party Phone
 *    A Party IMEI
 *    A Party IMSI
 *    Circle Id
 *
 * @since 3.7.0
 */
public class DataGenerator extends BaseOperator implements InputOperator
{
  public static int NUM_CIRCLES = 10;

  private Random r;
  private volatile int count = 0;
  private int limit = 15;

  public final transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<>();

  @Override
  public void setup(OperatorContext context)
  {
    r = new Random(System.currentTimeMillis());
  }

  @Override
  public void emitTuples()
  {
    if (count < limit) {
      output.emit(getRecord());
      count++;
    }
  }

  private byte[] getRecord()
  {
    String phone = getRandomNumber(10);
    String imsi = getHashInRange(phone, 15);
    String imei = getHashInRange(imsi, 15);
    String circleId = Math.abs(count) % NUM_CIRCLES + "";
    String record = "{" + "\"phone\":\"" + phone + "\","
        + "\"imei\":\"" + imei + "\","
        + "\"imsi\":\"" + imsi + "\","
        + "\"circleId\":" + circleId + "}";
    return record.getBytes();
  }

  private String getRandomNumber(int numDigits)
  {
    String retVal = (r.nextInt((9 - 1) + 1) + 1) + "";

    for (int i = 0; i < numDigits - 1; i++) {
      retVal += (r.nextInt((9 - 0) + 1) + 0);
    }
    return retVal;
  }

  private String getHashInRange(String s, int n)
  {
    StringBuilder retVal = new StringBuilder();
    for (int i = 0, j = 0; i < n && j < s.length(); i++, j++) {
      retVal.append(Math.abs(s.charAt(j) + "".hashCode()) % 10);
      if (j == s.length() - 1) {
        j = -1;
      }
    }
    return retVal.toString();
  }

  public int getLimit()
  {
    return limit;
  }

  public void setLimit(int limit)
  {
    this.limit = limit;
  }

  public int getCount()
  {
    return count;
  }

  public void setCount(int count)
  {
    this.count = count;
  }
}
