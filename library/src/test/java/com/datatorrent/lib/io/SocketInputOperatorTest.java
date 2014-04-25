package com.datatorrent.lib.io;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 * Functional test for {@link com.datatorrent.lib.io.AbstractSocketInputOperator}.
 */
public class SocketInputOperatorTest
{
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
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        SocketAddress port = new InetSocketAddress(serverPort);
        serverChannel.socket().bind(port);
        while (true) {
          SocketChannel clientChannel = serverChannel.accept();
          String response = "This is " + serverChannel.socket() + " on port " + serverChannel.socket().getLocalPort()+".";
          byte[] data = response.getBytes("UTF-8");
          ByteBuffer buffer = ByteBuffer.wrap(data);
          while (buffer.hasRemaining()) {
            clientChannel.write(buffer);
          }
          data =" This is server reporting".getBytes();
          buffer = ByteBuffer.wrap(data);
          while (buffer.hasRemaining()) {
            clientChannel.write(buffer);
          }
          clientChannel.close();
        }
      }
      catch (Exception e) {
        LOG.debug("server ", e);
      }
    }
  }

  @Test
  public void Test()
  {
    try {
      Thread server = new Thread(new Server(7898));
      server.start();
      // server.join();
      TestSocketInputOperator operator = new TestSocketInputOperator();
      operator.setHostname("localhost");
      operator.setPort(7898);
      operator.setScanIntervalInMilliSeconds(10);
      CollectorTestSink sink = new CollectorTestSink();
      operator.outputPort.setSink(sink);
      operator.setup(null);
      operator.activate(null);
      operator.beginWindow(0);
      Thread.sleep(100);
      operator.emitTuples();
      Thread.sleep(100);
      operator.emitTuples();
      Thread.sleep(100);
      operator.emitTuples();
      operator.endWindow();
      operator.deactivate();
      operator.teardown();
      Assert.assertEquals("This is ServerSocket[addr=/0:0:0:0:0:0:0:0,localport=7898] on port 7898. This is server reportin", sink.collectedTuples.get(0));
      Assert.assertEquals("g", sink.collectedTuples.get(1));
      server.interrupt();
      server.join();
    }
    catch (Exception e) {
      LOG.debug("exception", e);
    }
  }

  @Test
  public void TestWithSmallerBufferSize()
  {
    try {
      Thread server = new Thread(new Server(7899));
      server.start();
      // server.join();
      TestSocketInputOperator operator = new TestSocketInputOperator();
      operator.setHostname("localhost");
      operator.setPort(7899);
      operator.setScanIntervalInMilliSeconds(10);
      operator.setByteBufferSize(10);
      CollectorTestSink sink = new CollectorTestSink();
      operator.outputPort.setSink(sink);
      operator.setup(null);
      operator.activate(null);
      operator.beginWindow(0);
      Thread.sleep(100);
      for(int i = 0;i < 10; i++) {
        operator.emitTuples();
        Thread.sleep(100);
      }
      operator.endWindow();
      operator.deactivate();
      operator.teardown();
      Assert.assertEquals(10, sink.collectedTuples.size());
      Assert.assertEquals("This is Se", sink.collectedTuples.get(0));
      Assert.assertEquals("rverSocket", sink.collectedTuples.get(1));
      Assert.assertEquals("[addr=/0:0", sink.collectedTuples.get(2));
      Assert.assertEquals(":0:0:0:0:0", sink.collectedTuples.get(3));
      Assert.assertEquals(":0,localpo", sink.collectedTuples.get(4));
      Assert.assertEquals("rt=7899] o", sink.collectedTuples.get(5));
      Assert.assertEquals("n port 789", sink.collectedTuples.get(6));
      Assert.assertEquals("9. This is", sink.collectedTuples.get(7));
      Assert.assertEquals(" server re", sink.collectedTuples.get(8));
      Assert.assertEquals("porting", sink.collectedTuples.get(9));
      server.interrupt();
      server.join();
    }
    catch (Exception e) {
      LOG.debug("exception", e);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(SocketInputOperatorTest.class);
}
