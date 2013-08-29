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
package com.datatorrent.lib.helper;

import com.datatorrent.api.util.JacksonObjectMapperProvider;
import java.io.IOException;
import java.util.HashMap;
import javax.servlet.http.HttpServletRequest;
import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@SuppressWarnings("serial")
public class SamplePubSubWebSocketServlet extends WebSocketServlet
{

  private static final Logger LOG = LoggerFactory.getLogger(SamplePubSubWebSocketServlet.class);
  private ObjectMapper mapper = (new JacksonObjectMapperProvider()).getContext(null);

  private PubSubWebSocket subscriber;
  @SuppressWarnings("unused")
  private String topic;

  @Override
  public WebSocket doWebSocketConnect(HttpServletRequest hsr, String string)
  {
    return new PubSubWebSocket();
  }

  private class PubSubWebSocket implements WebSocket.OnTextMessage
  {

    private Connection connection;

    @SuppressWarnings("unchecked")
    @Override
    public void onMessage(String arg0)
    {
      try {
        HashMap<String, Object> map = mapper.readValue(arg0, HashMap.class);
        String type = (String)map.get("type");
        String topic = (String)map.get("topic");
         if (type.equals("subscribe")) {
            if (topic != null) {
              subscriber = this;
              topic = arg0;
            }
         } else if (type.equals("unsubscribe")) {
           subscriber = null;
           topic = null;
         } else if (type.equals("publish")) {
           Object data = map.get("data");
           if (data != null) {
             if (subscriber != null) {
              sendData(subscriber, topic, data);
             }
           }
         }
      } catch (Exception ex) {
          LOG.warn("Data read error", ex);
      }
    }

    @Override
    public void onOpen(Connection cnctn)
    {
      this.connection = cnctn;
    }

    @Override
    public void onClose(int i, String string)
    {
      if (subscriber == this) {
        subscriber = null;
        topic = null;
      }
    }

  }

  private synchronized void sendData(PubSubWebSocket webSocket, String topic, Object data)
  {
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("type", "data");
    map.put("topic", topic);
    map.put("data", data);
    try {
      webSocket.connection.sendMessage(mapper.writeValueAsString(map));
    }
    catch (IOException ex) {
      LOG.warn("Connection send error", ex);
    }
  }

}
