/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.singlejoin;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.InputOperator;
import java.util.ArrayList;
import java.util.Random;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class AgeGenerator implements InputOperator
{
  public transient DefaultOutputPort<ArrayList<Object>> output = new DefaultOutputPort<ArrayList<Object>>(this);
  private Random random;

  @Override
  public void emitTuples()
  {
    Integer id = random.nextInt(10);
    Integer age = id+20;
    ArrayList<Object> list = new ArrayList<Object>();
    list.add(id);
    list.add(age);
    output.emit(list);
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
