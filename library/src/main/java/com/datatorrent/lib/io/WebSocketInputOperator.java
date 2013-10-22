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

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.ShipContainingJars;

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
 * @since 0.3.2
 */
@ShipContainingJars(classes = {org.codehaus.jackson.JsonFactory.class,org.eclipse.jetty.websocket.WebSocket.class,
    org.eclipse.jetty.util.component.AggregateLifeCycle.class,
    org.eclipse.jetty.io.ByteArrayBuffer.class,org.eclipse.jetty.http.HttpParser.class})
public class WebSocketInputOperator extends SimpleSinglePortInputOperator<Map<String, String>> implements Runnable
{
  private static final Logger LOG = LoggerFactory.getLogger(WebSocketInputOperator.class);
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

  private transient boolean connectionClosed = false;
  private transient boolean shutdown = false;

  public void setUri(URI uri)
  {
    this.uri = uri;
  }

  @Override
  public void setup(OperatorContext context)
  {
    try {
      uri = URI.create(uri.toString()); // force reparse after deserialization

      factory.setBufferSize(8192);
      factory.start();

      //client = factory.newWebSocketClient();
      LOG.info("URL: {}", uri);
      // Handle asynchronous websocket client disconnects in future
      // differently to not use a monitor thread. 
      shutdown = false;
      monThread = new MonitorThread();
      monThread.start();
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void teardown()
  {
    shutdown = true;
    try {
      if (monThread != null) {
        monThread.join();
      }
    } catch (Exception ex) {
      LOG.error("Error joining monitor", ex );
    }

    if (factory != null) {
      factory.destroy();
    }
    super.teardown();
  }

  @SuppressWarnings("unchecked")
  public Map<String,String> convertMessageToMap(String message) throws IOException
  {
    return mapper.readValue(message, HashMap.class);
  }
  
  private transient MonitorThread monThread;
 
  private class MonitorThread extends Thread {
	public void run() {
	   while (!WebSocketInputOperator.this.shutdown) {
		try {
			sleep(1000);
			if (connectionClosed && !WebSocketInputOperator.this.shutdown) {
				WebSocketInputOperator.this.activate(null);
			}
		} catch (Exception ex) {
		}
	   }
	}
  }

  @Override
  public void run()
  {
    try {
      connectionClosed = false;
      client = factory.newWebSocketClient();
      connection = client.open(uri, new WebSocket.OnTextMessage()
      {
        @Override
        public void onMessage(String string)
        {
          LOG.debug("Got: " + string);
          try {
            Map<String, String> o = convertMessageToMap(string);
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
          LOG.debug("Connection connectionClosed.");
	  connectionClosed = true;
        }

      }).get(5, TimeUnit.SECONDS);
    }
    catch (Exception ex) {
      LOG.error("Error reading from " + uri, ex);
      connectionClosed = true;
    }

  }

}
