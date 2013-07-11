/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.helper;

import com.datatorrent.api.util.ObjectMapperString;
import com.datatorrent.api.util.PubSubWebSocketClient;
import static java.lang.Thread.sleep;
import java.net.URI;
import org.eclipse.jetty.websocket.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SamplePubSubWebSocketPublisher implements Runnable
{
  private static final String defaultUri = "ws://localhost:9090/pubsub";
  private URI uri;
  private ObjectMapperString payload = new ObjectMapperString("{\"hello\":\"world\"}");
  private String topic = "testTopic";

  public void setUri(URI uri)
  {
    this.uri = uri;
  }

  public void setPayload(ObjectMapperString payload)
  {
    this.payload = payload;
  }

  public void setTopic(String topic)
  {
    this.topic = topic;
  }

  @Override
  @SuppressWarnings("SleepWhileInLoop")
  public void run()
  {
    try {
      PubSubWebSocketClient wsClient = new PubSubWebSocketClient()
      {
        @Override
        public void onOpen(WebSocket.Connection connection)
        {
        }

        @Override
        public void onMessage(String type, String topic, Object data)
        {
        }

        @Override
        public void onClose(int code, String message)
        {
        }

      };
      if (uri == null) {
        uri = new URI(defaultUri);
      }
      wsClient.setUri(uri);
      wsClient.openConnection(1000);
      while (true) {
        wsClient.publish(topic, payload);
        sleep(1000);
      }
    }
    catch (InterruptedException ex) {
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static void main(String[] args) throws Exception
  {
    SamplePubSubWebSocketPublisher sp = new SamplePubSubWebSocketPublisher();
    sp.run();
  }

  private static final Logger logger = LoggerFactory.getLogger(SamplePubSubWebSocketPublisher.class);
}
