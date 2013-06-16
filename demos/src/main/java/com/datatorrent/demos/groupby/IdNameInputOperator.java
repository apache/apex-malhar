/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.groupby;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Context.OperatorContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class IdNameInputOperator implements InputOperator
{
  public transient DefaultOutputPort<HashMap<String, Object>> output = new DefaultOutputPort<HashMap<String, Object>>(this);
  private Random random;
  private String[] name = {"mark", "steve", "allen","bob","john","mary","hellen","christina","beyonce","alex"};
  private transient int interval;

  public void setInterval(int ms) {
    interval = ms;
  }

  @Override
  public void emitTuples()
  {
    Integer id = random.nextInt(name.length);
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("id",id);
    map.put("name", name[id]);
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
