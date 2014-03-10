/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.util.PubSubMessageCodec;
import com.datatorrent.lib.util.PubSubWebSocketClient;

/**
 * <p>PubSubWebSocketInputOperator class.</p>
 *
 * @since 0.3.2
 */
public class PubSubWebSocketInputOperator extends WebSocketInputOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(PubSubWebSocketInputOperator.class);
  private HashSet<String> topics = new HashSet<String>();
  private transient PubSubMessageCodec<Object> codec;

  public PubSubWebSocketInputOperator()
  {
    this.codec = new PubSubMessageCodec<Object>(mapper);
  }

  public void addTopic(String topic)
  {
    topics.add(topic);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Map<String, String> convertMessageToMap(String message) throws IOException
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
        connection.sendTextMessage(PubSubWebSocketClient.constructSubscribeMessage(topic, codec));
      }
    }
    catch (IOException ex) {
      LOG.error("Exception caught", ex);
    }
  }

}
