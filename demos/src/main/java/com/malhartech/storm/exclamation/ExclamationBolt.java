/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.storm.exclamation;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class ExclamationBolt extends BaseOperator
{
  public final transient DefaultInputPort<String> input = new DefaultInputPort<String>(this)
  {
    @Override
    public void process(String word)
    {
      output.emit(word + "!!!");
    }

  };
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>(this);
}
