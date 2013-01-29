/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.groupby;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.InputOperator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class IdAgeInputOperator implements InputOperator
{
  public transient DefaultOutputPort<HashMap<String, Integer>>output = new DefaultOutputPort<HashMap<String, Integer>>(this);
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
