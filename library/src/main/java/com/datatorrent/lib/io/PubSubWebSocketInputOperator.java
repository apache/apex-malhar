/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.io;

import com.malhartech.api.util.PubSubWebSocketClient;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class PubSubWebSocketInputOperator extends WebSocketInputOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(PubSubWebSocketInputOperator.class);
  private HashSet<String> topics = new HashSet<String>();

  public void addTopic(String topic)
  {
    topics.add(topic);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, String> convertMessageToMap(String message) throws IOException
  {
    Map<String, Object> map = mapper.readValue(message, HashMap.class);
    return (Map<String, String>)map.get("data");
  }

  @Override
  public void run()
  {
    super.run();
    try {
      for (String topic: topics) {
        connection.sendMessage(PubSubWebSocketClient.constructSubscribeMessage(topic, mapper));
      }
    }
    catch (IOException ex) {
      LOG.error("Exception caught", ex);
    }
  }

}
