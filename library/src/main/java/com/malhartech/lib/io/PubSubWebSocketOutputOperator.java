/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.util.PubSubWebSocketClient;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @param <T>
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class PubSubWebSocketOutputOperator<T> extends WebSocketOutputOperator<T>
{
  private static final Logger LOG = LoggerFactory.getLogger(PubSubWebSocketOutputOperator.class);
  private String topic = null;

  public void setTopic(String topic)
  {
    this.topic = topic;
  }

  @Override
  public String convertMapToMessage(T t) throws IOException
  {
    return PubSubWebSocketClient.constructPublishMessage(topic, t, mapper);
  }

}
