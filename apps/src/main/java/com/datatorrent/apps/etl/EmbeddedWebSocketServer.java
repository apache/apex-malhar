/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.apps.etl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketHandler;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;
import javax.validation.constraints.NotNull;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class EmbeddedWebSocketServer<DATA, CONTEXT extends Context> extends Server implements Component<Context>
{
  private transient final JsonFactory jsonFactory = new JsonFactory();
  protected transient final ObjectMapper mapper = new ObjectMapper(jsonFactory);

  protected SelectChannelConnector connector;
  protected WebSocketHandler webSocketHandler;
  List<EmbeddedWebSocket> clients = new ArrayList<EmbeddedWebSocket>();
  @NotNull
  protected WebSocketCallBackBase callBackBase;

  public WebSocketCallBackBase getCallBackBase()
  {
    return callBackBase;
  }

  public void setCallBackBase(WebSocketCallBackBase callBackBase)
  {
    this.callBackBase = callBackBase;
  }
  int port;

  @Override
  public void setup(Context context)
  {
    connector = new SelectChannelConnector();
    //connector.setPort(0);
    connector.setPort(8888); // test port
    addConnector(connector);
    webSocketHandler = new EmbeddedWebSocketHandler();
    setHandler(webSocketHandler);
  }

  public void process(DATA t)
  {
    String data = "hello world";
    for (EmbeddedWebSocket client : clients) {
      try {
        client.getConnection().sendMessage(data);
      }
      catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  protected void sendMessage(EmbeddedWebSocket client, String out)
  {
    try {
      client.getConnection().sendMessage(out);
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  protected void processMessage(EmbeddedWebSocket client, String message)
  {
      String response = callBackBase.onQuery(message);
      sendMessage(client, response);
  }

  @Override
  public void teardown()
  {
  }

  public class EmbeddedWebSocket implements WebSocket, WebSocket.OnTextMessage
  {
    private Connection connection;

    @Override
    public void onOpen(Connection connection)
    {
      this.connection = connection;
      // add to subscribers
      clients.add(this);
    }

    @Override
    public void onClose(int closeCode, String message)
    {
      // remove from subscribers
      clients.remove(this);
    }

    @Override
    public void onMessage(String data)
    {
      // received message from client
      processMessage(this, data);
    }

    public Connection getConnection()
    {
      return connection;
    }

  }

  public class EmbeddedWebSocketHandler extends WebSocketHandler
  {
    @Override
    public WebSocket doWebSocketConnect(HttpServletRequest request, String protocol)
    {
      return new EmbeddedWebSocket();
    }

  }
}
