/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io;

import com.datatorrent.api.Context.OperatorContext;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfigBean;
import com.ning.http.client.websocket.WebSocket;
import com.ning.http.client.websocket.WebSocketTextListener;
import com.ning.http.client.websocket.WebSocketUpgradeHandler;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang3.ClassUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads via WebSocket from given URL as input stream.&nbsp;
 * Incoming data is interpreted as JSONObject and converted to {@link java.util.Map}.
 * <p></p>
 * @displayName JSON Map Input
 * @category Input
 * @tags http, websocket
 *
 * @since 0.3.2
 */
public class WebSocketInputOperator<T> extends SimpleSinglePortInputOperator<T> implements Runnable
{
  private static final long serialVersionUID = 201506160829L;
  private static final Logger LOG = LoggerFactory.getLogger(WebSocketInputOperator.class);
  /**
   * Timeout interval for reading from server. 0 or negative indicates no timeout.
   */
  public int readTimeoutMillis = 0;
  //Do not make this @NotNull since null is a valid value for some child classes
  protected URI uri;
  private transient AsyncHttpClient client;
  private transient final JsonFactory jsonFactory = new JsonFactory();
  protected transient final ObjectMapper mapper = new ObjectMapper(jsonFactory);
  protected transient WebSocket connection;
  private transient boolean connectionClosed = false;
  private transient boolean shutdown = false;
  private int ioThreadMultiplier = 1;
  protected boolean skipNull = false;

  /**
   * Gets the URI for WebSocket connection
   *
   * @return the URI
   */
  public URI getUri()
  {
    return uri;
  }

  /**
   * Sets the URI for WebSocket connection
   *
   * @param uri
   */
  public void setUri(URI uri)
  {
    this.uri = uri;
  }

  /**
   * Gets the IO Thread multiplier for AsyncWebSocket connection
   *
   * @return the IO thread multiplier
   */
  public int getIoThreadMultiplier()
  {
    return ioThreadMultiplier;
  }

  /**
   * The number of threads to use for the websocket connection.
   *
   * @param ioThreadMultiplier
   */
  public void setIoThreadMultiplier(int ioThreadMultiplier)
  {
    this.ioThreadMultiplier = ioThreadMultiplier;
  }

  @Override
  public void setup(OperatorContext context)
  {
    try {
      uri = URI.create(uri.toString()); // force reparse after deserialization

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
    }
    catch (Exception ex) {
      LOG.error("Error joining monitor", ex);
    }
    super.teardown();
  }

  @SuppressWarnings("unchecked")
  protected T convertMessage(String message) throws IOException
  {
    return (T)mapper.readValue(message, Object.class);
  }

  private transient MonitorThread monThread;

  private class MonitorThread extends Thread
  {
    @Override
    public void run()
    {
      while (!WebSocketInputOperator.this.shutdown) {
        try {
          sleep(1000);
          if (connectionClosed && !WebSocketInputOperator.this.shutdown) {
            WebSocketInputOperator.this.activate(null);
          }
        }
        catch (Exception ex) {
        }
      }
    }

  }

  @Override
  public void run()
  {
    try {
      connectionClosed = false;
      AsyncHttpClientConfigBean config = new AsyncHttpClientConfigBean();
      config.setIoThreadMultiplier(ioThreadMultiplier);
      config.setApplicationThreadPool(Executors.newCachedThreadPool(new ThreadFactory()
      {
        private long count = 0;

        @Override
        public Thread newThread(Runnable r)
        {
          Thread t = new Thread(r);
          t.setName(ClassUtils.getShortClassName(this.getClass()) + "-AsyncHttpClient-" + count++);
          return t;
        }

      }));
      client = new AsyncHttpClient(config);
      connection = client.prepareGet(uri.toString()).execute(new WebSocketUpgradeHandler.Builder().addWebSocketListener(new WebSocketTextListener()
      {
        @Override
        public void onMessage(String string)
        {
          LOG.debug("Got: " + string);
          try {
            T o = convertMessage(string);
            if(!(skipNull && o == null)) {
              outputPort.emit(o);
            }
          }
          catch (IOException ex) {
            LOG.error("Got exception: ", ex);
          }
        }

        @Override
        public void onFragment(String string, boolean bln)
        {
          LOG.debug("onFragment");
        }

        @Override
        public void onOpen(WebSocket ws)
        {
          LOG.debug("Connection opened");
        }

        @Override
        public void onClose(WebSocket ws)
        {
          LOG.debug("Connection connectionClosed.");
          connectionClosed = true;
        }

        @Override
        public void onError(Throwable t)
        {
          LOG.error("Caught exception", t);
        }

      }).build()).get(5, TimeUnit.SECONDS);
    }
    catch (Exception ex) {
      LOG.error("Error reading from " + uri, ex);
      if (client != null) {
        client.close();
      }
      connectionClosed = true;
    }

  }

}
