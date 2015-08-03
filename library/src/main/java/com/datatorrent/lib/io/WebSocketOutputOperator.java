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

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.*;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfigBean;
import com.ning.http.client.websocket.WebSocket;
import com.ning.http.client.websocket.WebSocketTextListener;
import com.ning.http.client.websocket.WebSocketUpgradeHandler;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.ClassUtils;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;

/**
 * Reads via WebSocket from given URL as input stream.&nbsp;Incoming data is interpreted as JSONObject and converted to {@link java.util.Map}.
 * <p></p>
 * @displayName JSON Map Output
 * @category Output
 * @tags http, websocket
 *
 * @param <T> tuple type
 * @since 0.3.2
 */
public class WebSocketOutputOperator<T> extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(WebSocketOutputOperator.class);
  //Do not make this @NotNull since null is a valid value for some child classes
  protected URI uri;
  private transient AsyncHttpClient client;
  private transient final JsonFactory jsonFactory = new JsonFactory();
  protected transient final ObjectMapper mapper = new ObjectMapper(jsonFactory);
  protected transient WebSocket connection;
  private int ioThreadMultiplier = 1;
  private int numRetries = 3;
  private int waitMillisRetry = 5000;
  private long count = 0;

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
   * Gets the milliseconds to wait before retrying connection
   *
   * @return wait in milliseconds
   */
  public int getWaitMillisRetry()
  {
    return waitMillisRetry;
  }

  /**
   * Sets the milliseconds to wait before retrying connection
   *
   * @param waitMillisRetry
   */
  public void setWaitMillisRetry(int waitMillisRetry)
  {
    this.waitMillisRetry = waitMillisRetry;
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

  /**
   * Gets the number of retries of connection before the giving up
   *
   * @return the number of retries
   */
  public int getNumRetries()
  {
    return numRetries;
  }

  /**
   * Sets the number of retries of connection before the giving up
   *
   * @param numRetries
   */
  public void setNumRetries(int numRetries)
  {
    this.numRetries = numRetries;
  }

  /**
   * The input port
   */
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T t)
    {
      int countTries = 0;
      while (true) {
        try {
          if (connection == null || !connection.isOpen()) {
            LOG.warn("Connection is closed. Reconnecting...");
            client.close();
            openConnection();
          }
          connection.sendTextMessage(convertMapToMessage(t));
          break;
        }
        catch (Exception ex) {
          if (++countTries < numRetries) {
            LOG.debug("Caught exception", ex);
            LOG.warn("Send message failed ({}). Retrying ({}).", ex.getMessage(), countTries);
            if (connection != null) {
              connection.close();
            }
            if (waitMillisRetry > 0) {
              try {
                Thread.sleep(waitMillisRetry);
              }
              catch (InterruptedException ex1) {
              }
            }
          }
          else {
            throw new RuntimeException(ex);
          }
        }
      }
    }

  };

  private void openConnection() throws IOException, ExecutionException, InterruptedException, TimeoutException
  {
    final AsyncHttpClientConfigBean config = new AsyncHttpClientConfigBean();
    config.setIoThreadMultiplier(ioThreadMultiplier);
    config.setApplicationThreadPool(Executors.newCachedThreadPool(new ThreadFactory()
    {
      @Override
      public Thread newThread(Runnable r)
      {
        Thread t = new Thread(r);
        t.setName(ClassUtils.getShortClassName(this.getClass()) + "-AsyncHttpClient-" + count++);
        return t;
      }

    }));
    client = new AsyncHttpClient(config);
    uri = URI.create(uri.toString()); // force reparse after deserialization
    LOG.info("Opening URL: {}", uri);
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

  @Override
  public void setup(OperatorContext context)
  {
    try {
      openConnection();
    }
    catch (Exception ex) {
      LOG.warn("Cannot establish connection:", ex);
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
