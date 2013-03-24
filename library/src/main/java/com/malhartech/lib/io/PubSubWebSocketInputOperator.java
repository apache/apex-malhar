/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

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
  public Map<String, String> convertMessageToMap(String string) throws IOException
  {
    Map<String, Object> map = mapper.readValue(string, HashMap.class);
    return (Map<String, String>)map.get("data");
  }

  @Override
  public void run()
  {
    super.run();
    HashMap<String, String> map = new HashMap<String, String>();
    map.put("type", "subscribe");
    try {
      for (String topic: topics) {
        map.put("topic", topic);
        String message = mapper.writeValueAsString(map);
        connection.sendMessage(message);
      }
    }
    catch (IOException ex) {
      LOG.error("Exception caught", ex);
    }
  }

}
