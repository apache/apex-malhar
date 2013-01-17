/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.storm.samplestream;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class PrintBolt extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(PrintBolt.class);
  public transient DefaultInputPort<String> input = new DefaultInputPort<String>(this)
  {
    @Override
    public void process(String tuple)
    {
      logger.debug(tuple);
    }
  };

}
