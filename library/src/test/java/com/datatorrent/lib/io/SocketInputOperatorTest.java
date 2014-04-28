package com.datatorrent.lib.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
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
  private static String inputData1 = "We hd previously reported that Blue Apron was raising $30 million on the $500 million valuation that had been reported by Fortune, " + "but those numbers seem to be preliminary. Other sources have said that cofounder Matt Salzberg was looking for a $500 million valuation " + "for this round, though he has apparently settled for a touch less. The exchange, which will allow mobile publishers to run ads from " + "multiple ad networks and ad buyers, addresses each of those issues, Jaffer argued. On ad ";

  private static String inputData2 = "quality, Vungle will only serve high-resolution videos of 15 seconds or fewer. On latency, the company says it serves the " + "ads “in less time than it takes the human eye to blink.” To address brand safety, the exchange will give advertisers control " + "over where their ads can run, and it has also been built to support new video standards and formats that emerge. Lastly, the company " + "says its algorithms can optimize the mix between app install ads and brand ads.";

  private static String inputData3 = "Many of the world's largest and fastest-growing organizations including Facebook, Google, Adobe, Alcatel " + "Lucent and Zappos rely on MySQL to save time and money powering their high-volume Web sites, business-critical systems and packaged " + "software. Below you will find valuable resources including case studies and white papers that will help you implement cost-effective " + "database solutions using MySQL.";

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
          while(line != null){
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
      Thread.sleep(10000);
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
      Thread.sleep(1000);
      operator.emitTuples();
      Thread.sleep(1000);
      operator.emitTuples();
      operator.endWindow();
      operator.deactivate();
      operator.teardown();
      String outputString = (String) sink.collectedTuples.get(0);
      Assert.assertEquals(strBuffer.substring(0, outputString.length()), sink.collectedTuples.get(0));
      int length  = outputString.length();
      outputString = (String) sink.collectedTuples.get(1);
      Assert.assertEquals(strBuffer.substring(length, length+outputString.length()), sink.collectedTuples.get(1));
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
      Thread.sleep(10000);
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
      Thread.sleep(1000);
      for (int i = 0; i < 10; i++) {
        operator.emitTuples();
        Thread.sleep(1000);
      }
      operator.endWindow();
      operator.deactivate();
      operator.teardown();
      Assert.assertEquals(10, sink.collectedTuples.size());
      for (int i = 0; i < 10; i++) {
        Assert.assertEquals(strBuffer.substring(i * 10, (i + 1) * 10), sink.collectedTuples.get(i));
      }
      server.interrupt();
      server.join();
    }
    catch (Exception e) {
      LOG.debug("exception", e);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(SocketInputOperatorTest.class);
}
