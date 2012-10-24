/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.kafka;

import kafka.javaapi.producer.Producer;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.OperatorConfiguration;

public class KafkaOutputOperator extends BaseOperator
{
  @Override
  public void setup(OperatorConfiguration config)
  {
    Producer producer;
    throw new UnsupportedOperationException("Not supported yet.");
  }

}
