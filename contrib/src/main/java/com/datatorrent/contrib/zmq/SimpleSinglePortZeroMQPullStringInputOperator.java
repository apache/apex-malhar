/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.zmq;

import com.malhartech.contrib.zmq.SimpleSinglePortZeroMQPullInputOperator;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class SimpleSinglePortZeroMQPullStringInputOperator extends SimpleSinglePortZeroMQPullInputOperator<String>
{
  private SimpleSinglePortZeroMQPullStringInputOperator()
  {
    super("INVALID");
  }

  public SimpleSinglePortZeroMQPullStringInputOperator(String addr)
  {
    super(addr);
  }

  @Override
  protected String convertFromBytesToTuple(byte[] bytes)
  {
    return new String(bytes);
  }

}
