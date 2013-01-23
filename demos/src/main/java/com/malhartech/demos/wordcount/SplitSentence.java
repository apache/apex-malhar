/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.wordcount;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class SplitSentence extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(SplitSentence.class);
  public transient DefaultInputPort<String> input = new DefaultInputPort<String>(this)
  {
    @Override
    public void process(String line)
    {
      for(int i=0; i<7; i++ ) {
        String str = line.substring(i*10, i*10+10);
        output.emit(str);
      }
      String str = line.substring(7*10);
      output.emit(str);
    }
  };

  public transient DefaultOutputPort<String> output = new DefaultOutputPort<String>(this);
  @Override
  public void setup(OperatorContext context)
  {
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
  public void teardown()
  {
  }
}
