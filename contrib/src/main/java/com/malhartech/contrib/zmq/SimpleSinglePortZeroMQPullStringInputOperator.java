/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.zmq;

import com.malhartech.contrib.zmq.SimpleSinglePortZeroMQPullInputOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class SimpleSinglePortZeroMQPullStringInputOperator extends SimpleSinglePortZeroMQPullInputOperator<String>
{
  private static final Logger logger = LoggerFactory.getLogger(SimpleSinglePortZeroMQPullStringInputOperator.class);

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
