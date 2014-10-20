/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.contrib.goldengate.lib;

import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import kafka.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaInput extends  KafkaSinglePortStringInputOperator
{
  private static final Logger logger = LoggerFactory.getLogger(KafkaInput.class);

  @Override
  public String getTuple(Message message)
  {
    String result = super.getTuple(message);
    logger.debug("recieved message {} " + result);
    return result;
  }
}