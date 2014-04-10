/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.etl;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketHandler;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class EmbeddedWebSocketServer<TUPLE,CONTEXT extends Context> extends Server implements Component<Context>
{
  private SelectChannelConnector connector;
  private WebSocketHandler webSocketHandler;
  List<EmbeddedWebSocket> clients = new ArrayList<EmbeddedWebSocket>();
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

  public void process(TUPLE t) {
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

  @Override
  public void teardown()
  {

  }

  public class EmbeddedWebSocket implements WebSocket, WebSocket.OnTextMessage {

    private Connection connection;

    @Override
    public void onOpen(Connection connection)
    {
      this.connection = connection;
      // add to subscribers
      clients.add(this);
      // send object schema
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
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public Connection getConnection()
    {
      return connection;
    }
  }

  public class EmbeddedWebSocketHandler extends WebSocketHandler {

    @Override
    public WebSocket doWebSocketConnect(HttpServletRequest request, String protocol)
    {
      return new EmbeddedWebSocket();
    }
  }
}
