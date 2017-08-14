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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator.ActivationListener;

/**
 * This is the base implementation for an input operator which reads from a network socket.&nbsp;
 * Subclasses must implement the method that is used to process incoming bytes from the socket.
 * <p>
 * <b>Ports</b>:</br> <b>outputPort</b>: emits &lt;<T></T>&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>hostname</b></br> <b>port</b></br> <b>byteBufferSize</b></br> <b>scanIntervalInMilliSeconds</b></br>
 * </p>
 * @displayName Abstract Socket Input
 * @category Input
 * @tags socket, input operator
 *
 * @param <T>
 * @since 0.9.5
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class AbstractSocketInputOperator<T> implements InputOperator, ActivationListener<OperatorContext>
{
  /* The host to which to connect */
  private String hostname;
  /* The port on to connect to */
  private int port;
  /* the time interval for periodically scanning, Default is 1 sec = 1000ms */
  private int scanIntervalInMilliSeconds = 1000;
  /* the size of the ByteBuffer */
  private int byteBufferSize = 1024;

  private transient Selector selector;
  private transient SocketChannel channel;
  private transient SelectionKey key;
  private transient Thread scanThread = new Thread(new SelectorScanner());
  private transient ByteBuffer byteBuffer;
  private transient Lock lock;

  /**
   * This is the output port which emits tuples read from a socket.
   */
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();

  public int getByteBufferSize()
  {
    return byteBufferSize;
  }

  public void setByteBufferSize(int byteBufferSize)
  {
    this.byteBufferSize = byteBufferSize;
  }

  public String getHostname()
  {
    return hostname;
  }

  public int getPort()
  {
    return port;
  }

  public void setHostname(String hostname)
  {
    this.hostname = hostname;
  }

  public void setPort(int port)
  {
    this.port = port;
  }

  public int getScanIntervalInMilliSeconds()
  {
    return scanIntervalInMilliSeconds;
  }

  public void setScanIntervalInMilliSeconds(int scanIntervalInMilliSeconds)
  {
    this.scanIntervalInMilliSeconds = scanIntervalInMilliSeconds;
  }

  @Override
  public void emitTuples()
  {
    lock.lock();
    byteBuffer.flip();
    processBytes(byteBuffer);
    byteBuffer.flip();
    byteBuffer.clear();
    lock.unlock();

  }

  public abstract void processBytes(ByteBuffer byteBuffer);

  @Override
  public void beginWindow(long l)
  {

  }

  @Override
  public void endWindow()
  {

  }

  @Override
  public void setup(OperatorContext operatorContext)
  {

  }

  @Override
  public void teardown()
  {

  }

  @Override
  public void activate(OperatorContext operatorContext)
  {
    try {
      selector = Selector.open();
      channel = SocketChannel.open();
      channel.configureBlocking(false);
      channel.connect(new InetSocketAddress(hostname, port));
      channel.register(selector, SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    lock = new ReentrantLock();
    scanThread.start();
    byteBuffer = ByteBuffer.allocate(byteBufferSize);
  }

  @Override
  public void deactivate()
  {
    try {
      channel.close();
      selector.close();
      scanThread.interrupt();
      scanThread.join();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Thread that periodically looks for more data on the port.
   */
  public class SelectorScanner implements Runnable
  {
    public void run()
    {

      boolean acquiredLock = false;
      try {
        while (true) {
          selector.select();
          Set<SelectionKey> selectedKeys = selector.selectedKeys();
          Iterator<SelectionKey> keyIterator = selectedKeys.iterator();
          while (keyIterator.hasNext()) {
            SelectionKey nextKey = keyIterator.next();
            keyIterator.remove();
            if (nextKey.isConnectable()) {
              SocketChannel sChannel = (SocketChannel)nextKey.channel();
              sChannel.finishConnect();
            }
            if (nextKey.isReadable()) {
              SocketChannel sChannel = (SocketChannel)nextKey.channel();
              lock.lock();
              acquiredLock = true;
              sChannel.read(byteBuffer);
              lock.unlock();
              acquiredLock = false;
            }
          }
          // Sleep for Scan interval
          Thread.sleep(scanIntervalInMilliSeconds);
        }
      } catch (Exception e) {
        if (acquiredLock) {
          lock.unlock();
        }
      }
    }
  }
}
