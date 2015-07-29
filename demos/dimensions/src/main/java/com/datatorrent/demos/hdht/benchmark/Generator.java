/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.hdht.benchmark;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.contrib.hdht.MutableKeyValue;

import javax.validation.constraints.Min;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Generator
 *
 * @category Test Bench
 * @since 2.0.0
 */
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
