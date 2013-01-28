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
public class NameInputOperator implements InputOperator
{
  public transient DefaultOutputPort<ArrayList<Object>> output = new DefaultOutputPort<ArrayList<Object>>(this);
  private Random random;
  private String[] name = {"mark", "steve", "allen","bob","john","mary","hellen","christina","beyonce","alex"};

  @Override
  public void emitTuples()
  {
    Integer id = random.nextInt(name.length);
    ArrayList<Object> list = new ArrayList<Object>();
    list.add(id);
    list.add(name[id]);
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
