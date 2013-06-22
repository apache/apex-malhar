/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.groupby;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Context.OperatorContext;

import java.util.HashMap;
import java.util.Random;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class IdAgeInputOperator implements InputOperator
{
  public transient DefaultOutputPort<HashMap<String, Integer>>output = new DefaultOutputPort<HashMap<String, Integer>>();
  private Random random;
  private transient int interval;

  public void setInterval(int ms) {
    interval = ms;
  }

  @Override
  public void emitTuples()
  {
    Integer id = random.nextInt(10);

    Integer age = id+20;
    HashMap<String, Integer> map = new HashMap<String, Integer>();
    map.put("id", id);
    map.put("age", age);
    output.emit(map);
    try {
      Thread.sleep(interval);
    }
    catch (InterruptedException ex) {
      System.out.println(ex.toString());
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    random = new Random(System.nanoTime());
  }

  @Override
  public void teardown()
  {
  }
}
