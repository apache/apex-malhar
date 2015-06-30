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

import java.net.URI;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;

import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketClient;
import org.eclipse.jetty.websocket.WebSocketClientFactory;
import org.junit.Assert;
import org.junit.Test;

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
