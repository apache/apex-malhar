/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.io;

import com.google.common.collect.Lists;
import java.net.URI;
import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketClient;
import org.eclipse.jetty.websocket.WebSocketClientFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class WebSocketServerInputOperatorTest
{
  @Test
  public void simpleTest() throws Exception
  {
    final int port = 6666;
    String connectionURI = "ws://localhost:" + port + WebSocketServerInputOperator.DEFAULT_EXTENSION;

    final String message = "hello world";

    WebSocketServerInputOperator wssio = new TestWSSIO();
    wssio.setPort(port);
    wssio.setup(null);

    WebSocketClientFactory clientFactory = new WebSocketClientFactory();
    clientFactory.start();
    WebSocketClient client = new WebSocketClient(clientFactory);

    Future<WebSocket.Connection> connectionFuture = client.open(new URI(connectionURI), new TestWebSocket());
    WebSocket.Connection connection = connectionFuture.get(5, TimeUnit.SECONDS);

    connection.sendMessage(message);

    long startTime = System.currentTimeMillis();

    while(startTime + 10000 > System.currentTimeMillis()) {
      if(TestWSSIO.messages.size() >= 1) {
        break;
      }

      Thread.sleep(100);
    }

    Assert.assertEquals("The number of messages recieved is incorrect.", 1, TestWSSIO.messages.size());
    Assert.assertEquals("Incorrect message received", message, TestWSSIO.messages.get(0));

    connection.close();
    wssio.teardown();
  }

  private static class TestWSSIO extends WebSocketServerInputOperator
  {
    public static List<String> messages = Lists.newArrayList();

    @Override
    public void processMessage(String data)
    {
      messages.add(data);
    }
  }

  public static class TestWebSocket implements WebSocket
  {
    public TestWebSocket()
    {
    }

    @Override
    public void onOpen(Connection connection)
    {
      //Do nothing
    }

    @Override
    public void onClose(int closeCode, String message)
    {
      //Do nothing
    }
  }
}
