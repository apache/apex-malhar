/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.ShipContainingJars;
import com.malhartech.api.Context.OperatorContext;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
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
 */
@ShipContainingJars(classes = {org.codehaus.jackson.JsonFactory.class,org.eclipse.jetty.websocket.WebSocket.class})
public class WebSocketInputOperator extends SimpleSinglePortInputOperator<Map<String, String>> implements Runnable
{
  private static final Logger LOG = LoggerFactory.getLogger(WebSocketInputOperator.class);
  /**
   * Timeout interval for reading from server. 0 or negative indicates no timeout.
   */
  public int readTimeoutMillis = 0;

  @NotNull
  private URI channelUrl;

  private transient final WebSocketClientFactory factory = new WebSocketClientFactory();
  private transient WebSocketClient client;
  private transient final JsonFactory jsonFactory = new JsonFactory();
  private transient final ObjectMapper mapper = new ObjectMapper(jsonFactory);

  public void setUrl(URI u)
  {
    channelUrl = u;
  }

  @Override
  public void setup(OperatorContext context)
  {
    try {
      factory.setBufferSize(8192);
      factory.start();

      client = factory.newWebSocketClient();
      LOG.info("URL: {}", channelUrl);
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

  @Override
  public void run()
  {
    try {
      client.open(channelUrl, new WebSocket.OnTextMessage()
      {
        @Override
        public void onMessage(String string)
        {
          LOG.debug("Got: " + string);
          try {
            Map<String, String> o = mapper.readValue(string, HashMap.class);
            outputPort.emit(o);
          }
          catch (IOException ex) {
            LOG.error("Got exception: ", ex);
          }
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
      LOG.error("Error reading from " + channelUrl, ex);
    }

  }

}
