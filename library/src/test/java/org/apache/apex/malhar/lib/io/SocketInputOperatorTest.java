/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.io;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

/**
 * Functional test for {@link org.apache.apex.malhar.lib.io.AbstractSocketInputOperator}.
 */
public class SocketInputOperatorTest
{
  private static String testData = "src/test/resources/SocketInputOperatorTest.txt";
  private StringBuffer strBuffer = new StringBuffer();

  public class TestSocketInputOperator extends AbstractSocketInputOperator<String>
  {
    @Override
    public void processBytes(ByteBuffer byteBuffer)
    {
      final byte[] bytes = new byte[byteBuffer.remaining()];
      byteBuffer.duplicate().get(bytes);
      outputPort.emit(new String(bytes));
    }
  }

  public class Server implements Runnable
  {
    private int serverPort;

    Server(int port)
    {
      this.serverPort = port;
    }

    @Override
    public void run()
    {
      try {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(testData)));
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        SocketAddress port = new InetSocketAddress(serverPort);
        serverChannel.socket().bind(port);
        while (true) {
          SocketChannel clientChannel = serverChannel.accept();
          String line = reader.readLine();
          ByteBuffer buffer;
          byte[] data;
          while (line != null) {
            strBuffer.append(line);
            data = line.getBytes();
            buffer = ByteBuffer.wrap(data);
            while (buffer.hasRemaining()) {
              clientChannel.write(buffer);
            }
            line = reader.readLine();
          }
          reader.close();
          clientChannel.close();
        }
      } catch (Exception e) {
        //fixme
      }
    }
  }

  @Test
  public void Test()
  {
    try {
      Thread server = new Thread(new Server(7898));
      server.start();
      Thread.sleep(1000);
      // server.join();
      TestSocketInputOperator operator = new TestSocketInputOperator();
      operator.setHostname("localhost");
      operator.setPort(7898);
      operator.setScanIntervalInMilliSeconds(10);
      CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
      operator.outputPort.setSink(sink);
      operator.setup(null);
      operator.activate(null);
      operator.beginWindow(0);
      Thread.sleep(1000);
      operator.emitTuples();
      Thread.sleep(1000);
      operator.emitTuples();
      operator.endWindow();
      operator.deactivate();
      operator.teardown();
      String outputString = (String)sink.collectedTuples.get(0);
      Assert.assertEquals(strBuffer.substring(0, outputString.length()), sink.collectedTuples.get(0));
      int length = outputString.length();
      outputString = (String)sink.collectedTuples.get(1);
      Assert.assertEquals(strBuffer.substring(length, length + outputString.length()), sink.collectedTuples.get(1));
      server.interrupt();
      server.join();
      Thread.sleep(1000);
    } catch (Exception e) {
      LOG.debug("exception", e);
    }
  }

  @Test
  public void TestWithSmallerBufferSize()
  {
    try {
      Thread server = new Thread(new Server(7899));
      server.start();
      Thread.sleep(1000);
      TestSocketInputOperator operator = new TestSocketInputOperator();
      operator.setHostname("localhost");
      operator.setPort(7899);
      operator.setScanIntervalInMilliSeconds(10);
      operator.setByteBufferSize(10);
      CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
      operator.outputPort.setSink(sink);
      operator.setup(null);
      operator.activate(null);
      operator.beginWindow(0);
      Thread.sleep(1000);
      for (int i = 0; i < 10; i++) {
        operator.emitTuples();
        Thread.sleep(1000);
      }
      operator.endWindow();
      operator.deactivate();
      operator.teardown();
      Assert.assertEquals(10, sink.collectedTuples.size());
      int endIndex = 0;
      int start = 0;
      for (int i = 0; i < 10; i++) {
        endIndex += ((String)sink.collectedTuples.get(i)).length();
        Assert.assertEquals(strBuffer.substring(start, endIndex), sink.collectedTuples.get(i));
        start = endIndex;
      }
      server.interrupt();
      server.join();
      Thread.sleep(1000);
    } catch (Exception e) {
      LOG.debug("exception", e);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(SocketInputOperatorTest.class);
}
