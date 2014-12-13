/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.demos.dimensions.benchmark;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.contrib.hdht.MutableKeyValue;

import javax.validation.constraints.Min;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Generator extends BaseOperator implements InputOperator
{
  /* Following parameters controls rate of tuples generation */
  private int tupleBlast = 1000;
  private int sleepms = 0;

  /* Length of value */
  @Min(8)
  private int valLen = 1000;

  /* Cardinality of keys, without timestamp filed */
  private long cardinality = 10000;


  private transient byte[] val;


  public int getTupleBlast()
  {
    return tupleBlast;
  }

  public void setTupleBlast(int tupleBlast)
  {
    this.tupleBlast = tupleBlast;
  }

  public int getSleepms()
  {
    return sleepms;
  }

  public void setSleepms(int sleepms)
  {
    this.sleepms = sleepms;
  }

  public transient DefaultOutputPort<MutableKeyValue> out = new DefaultOutputPort<MutableKeyValue>();

  public int getValLen()
  {
    return valLen;
  }

  public void setValLen(int valLen)
  {
    this.valLen = valLen;
  }

  public long getCardinality()
  {
    return cardinality;
  }

  public void setCardinality(long cardinality)
  {
    this.cardinality = cardinality;
  }

  private static final Random random = new Random();

  @Override public void emitTuples()
  {
    long timestamp = TimeUnit.MINUTES.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    for(int i = 0; i < tupleBlast; i++)
    {
      long longKey = Math.abs(random.nextLong());
      longKey = cardinality == 0? longKey : longKey % cardinality;
      byte[] key = ByteBuffer.allocate(16).putLong(timestamp).putLong(longKey).array();
      ByteBuffer.wrap(val).putLong(random.nextLong());
      MutableKeyValue pair = new MutableKeyValue(key, val);
      out.emit(pair);
    }
    try {
      if (sleepms != 0)
        Thread.sleep(sleepms);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  @Override public void setup(Context.OperatorContext operatorContext)
  {
    val = ByteBuffer.allocate(valLen).putLong(1234).array();
  }

}
