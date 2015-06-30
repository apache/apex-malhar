/*
 * Copyright (c) 2015 DataTorrent, Inc.
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
import com.datatorrent.api.InputOperator;
import com.datatorrent.netlet.util.DTThrowable;
import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketServlet;

public abstract class WebSocketServerInputOperator implements InputOperator
{
  public static final String DEFAULT_EXTENSION = "/in";

  @Min(1)
  private int port;
  @NotNull
  private String extension = DEFAULT_EXTENSION;

  private transient Server server;

  public WebSocketServerInputOperator()
  {
  }

  @Override
  public void emitTuples()
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    server = new Server(port);
    DefaultWebSocketServlet servlet = new DefaultWebSocketServlet();
    ServletHolder sh = new ServletHolder(servlet);
    ServletContextHandler contextHandler = new ServletContextHandler(server, "/", ServletContextHandler.SESSIONS);
    contextHandler.addServlet(sh, extension);

    try {
      server.start();
    }
    catch(Exception ex) {
      DTThrowable.rethrow(ex);
    }
  }

  @Override
  public void teardown()
  {
    try {
      server.stop();
    }
    catch(Exception ex) {
      DTThrowable.rethrow(ex);
    }
  }

  /**
   * @return the port
   */
  public int getPort()
  {
    return port;
  }

  /**
   * @param port the port to set
   */
  public void setPort(int port)
  {
    this.port = port;
  }

  /**
   * @return the extension
   */
  public String getExtension()
  {
    return extension;
  }

  /**
   * @param extension the extension to set
   */
  public void setExtension(String extension)
  {
    this.extension = extension;
  }

  public abstract void processMessage(String data);

  private class DefaultWebSocketServlet extends WebSocketServlet
  {
    private static final long serialVersionUID = 201503061010L;

    @Override
    public WebSocket doWebSocketConnect(HttpServletRequest request, String protocol)
    {
      return new DataSinkWebSocket();
    }
  }

  private class DataSinkWebSocket implements WebSocket.OnTextMessage
  {
    public DataSinkWebSocket()
    {
    }

    @Override
    public void onMessage(String data)
    {
      processMessage(data);
    }

    @Override
    public void onOpen(Connection connection)
    {
    }

    @Override
    public void onClose(int closeCode, String message)
    {
    }
  }
}
