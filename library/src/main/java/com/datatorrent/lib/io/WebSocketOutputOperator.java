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

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.ShipContainingJars;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfigBean;
import com.ning.http.client.websocket.WebSocket;
import com.ning.http.client.websocket.WebSocketTextListener;
import com.ning.http.client.websocket.WebSocketUpgradeHandler;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
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
 * @since 0.3.2
 */
@ShipContainingJars(classes = {com.ning.http.client.websocket.WebSocket.class})
public class WebSocketOutputOperator<T> extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(WebSocketOutputOperator.class);
  /**
   * Timeout interval for reading from server. 0 or negative indicates no timeout.
   */
  public int readTimeoutMillis = 0;
  @NotNull
  private URI uri;
  private transient AsyncHttpClient client;
  private transient final JsonFactory jsonFactory = new JsonFactory();
  protected transient final ObjectMapper mapper = new ObjectMapper(jsonFactory);
  protected transient WebSocket connection;
  private int ioThreadMultiplier = 1;

  public void setUri(URI uri)
  {
    this.uri = uri;
  }

  public void setIoThreadMultiplier(int ioThreadMultiplier)
  {
    this.ioThreadMultiplier = ioThreadMultiplier;
  }

  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T t)
    {
      try {
        connection.sendTextMessage(convertMapToMessage(t));
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
    AsyncHttpClientConfigBean config = new AsyncHttpClientConfigBean();
    config.setIoThreadMultiplier(ioThreadMultiplier);
    client = new AsyncHttpClient(config);
    try {
      uri = URI.create(uri.toString()); // force reparse after deserialization
      LOG.info("URL: {}", uri);
      connection = client.prepareGet(uri.toString()).execute(new WebSocketUpgradeHandler.Builder().addWebSocketListener(new WebSocketTextListener()
      {
        @Override
        public void onMessage(String string)
        {
        }

        @Override
        public void onFragment(String string, boolean bln)
        {
        }

        @Override
        public void onOpen(WebSocket ws)
        {
          LOG.debug("Connection opened");
        }

        @Override
        public void onClose(WebSocket ws)
        {
          LOG.debug("Connection closed.");
        }

        @Override
        public void onError(Throwable t)
        {
          LOG.error("Caught exception", t);
        }

      }).build()).get(5, TimeUnit.SECONDS);
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void teardown()
  {
    super.teardown();
    if (client != null) {
      client.close();
    }
  }

  public String convertMapToMessage(T t) throws IOException
  {
    return mapper.writeValueAsString(t);
  }

}
