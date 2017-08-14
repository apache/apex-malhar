/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.io;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.common.util.PubSubMessageCodec;

/**
 * This operator reads JSON objects from the given URL and converts them into maps.
 * <p></p>
 * @displayName Pub Sub Web Socket Input
 * @category Input
 * @tags http, websocket
 *
 * @since 0.3.2
 */
public class PubSubWebSocketInputOperator<T> extends WebSocketInputOperator<T>
{
  private static final Logger LOG = LoggerFactory.getLogger(PubSubWebSocketInputOperator.class);
  private String topic = null;
  private transient PubSubMessageCodec<Object> codec;

  public PubSubWebSocketInputOperator()
  {
    this.codec = new PubSubMessageCodec<Object>(mapper);
  }

  /**
   * The pub sub topic to subscribe to.
   * @param topic The pub sub topic to subscribe to.
   */
  public void setTopic(String topic)
  {
    this.topic = topic;
  }

  /**
   * Gets pub sub topic to subscribe to.
   * @return The pub sub topic to subscribe to.
   */
  @NotNull
  public String getTopic()
  {
    return topic;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected T convertMessage(String message) throws IOException
  {
    Map<String, Object> map = mapper.readValue(message, HashMap.class);
    return (T)map.get("data");
  }

  @Override
  public void run()
  {
    super.run();
    try {
      connection.sendMessage(PubSubMessageCodec.constructSubscribeMessage(topic, codec));
    } catch (IOException ex) {
      LOG.error("Exception caught", ex);
    }
  }

}
