/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.kafka;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import kafka.javaapi.producer.Producer;

public class KafkaOutputOperator extends BaseOperator
{
  @Override
  public void setup(OperatorContext context)
  {
    Producer producer;
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
