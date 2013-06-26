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
package com.datatorrent.lib.io;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.ShipContainingJars;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocket.Connection;
import org.eclipse.jetty.websocket.WebSocketClient;
import org.eclipse.jetty.websocket.WebSocketClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Reads via WebSocket from given URL as input stream<p>
 * <br>
 * Incoming data is interpreted as JSONObject and converted to {@link java.util.Map}.<br>
 * <br>
 *
 * @param <T> tuple type
 */
@ShipContainingJars(classes = {org.codehaus.jackson.JsonFactory.class, org.eclipse.jetty.websocket.WebSocket.class})
public class WebSocketOutputOperator<T> extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(WebSocketOutputOperator.class);
  /**
   * Timeout interval for reading from server. 0 or negative indicates no timeout.
   */
  public int readTimeoutMillis = 0;
  @NotNull
  private URI uri;
  private transient final WebSocketClientFactory factory = new WebSocketClientFactory();
  private transient WebSocketClient client;
  private transient final JsonFactory jsonFactory = new JsonFactory();
  protected transient final ObjectMapper mapper = new ObjectMapper(jsonFactory);
  protected transient Connection connection;

  public void setUri(URI uri)
  {
    this.uri = uri;
  }

  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T t)
    {
      try {
        connection.sendMessage(convertMapToMessage(t));
      }
      catch (IOException ex) {
        LOG.error("error sending message through web socket", ex);
        throw new RuntimeException(ex);
      }
    }

  };

  @Override
  public void setup(OperatorContext context)
  {
    try {
      uri = URI.create(uri.toString()); // force reparse after deserialization

      factory.setBufferSize(8192);
      factory.start();

      client = factory.newWebSocketClient();
      LOG.info("URL: {}", uri);
      connection = client.open(uri, new WebSocket.OnTextMessage()
      {
        @Override
        public void onMessage(String string)
        {
        }

        @Override
        public void onOpen(Connection cnctn)
        {
          LOG.debug("Connection opened");
        }

        @Override
        public void onClose(int i, String string)
        {
          LOG.debug("Connection closed.");
        }

      }).get(5, TimeUnit.SECONDS);
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void teardown()
  {
    if (factory != null) {
      factory.destroy();
    }
    super.teardown();
  }

  public String convertMapToMessage(T t) throws IOException
  {
    return mapper.writeValueAsString(t);
  }

}
