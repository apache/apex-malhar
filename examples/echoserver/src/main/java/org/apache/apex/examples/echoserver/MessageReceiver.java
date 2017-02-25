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
package org.apache.apex.examples.echoserver;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

/**
 * @since 2.1.0
 */
public class MessageReceiver implements InputOperator, NetworkManager.ChannelListener<DatagramChannel>
{
  private static final Logger logger = LoggerFactory.getLogger(MessageReceiver.class);

  private transient NetworkManager.ChannelAction<DatagramChannel> action;

  //Need the sender info, using a packet for now instead of the buffer
  private transient ByteBuffer buffer;
  //private transient DatagramPacket packet;

  private int port = 9000;
  private int maxMesgSize = 512;
  private int inactiveWait = 10;
  private boolean readReady = false;

  @Override
  public void emitTuples()
  {
    boolean emitData = false;
    if (readReady) {
      //DatagramSocket socket = action.channelConfiguration.socket;
      try {
        //socket.receive(packet);
        DatagramChannel channel = action.channelConfiguration.channel;
        SocketAddress address = channel.receive(buffer);
        if (address != null) {
          /*
          StringBuilder sb = new StringBuilder();
          buffer.rewind();
          while (buffer.hasRemaining()) {
            sb.append(buffer.getChar());
          }
          String mesg = sb.toString();
          */
          buffer.flip();
          String mesg = new String(buffer.array(), 0, buffer.limit());
          logger.info("Message {}", mesg);
          Message message = new Message();
          message.message = mesg;
          message.socketAddress = address;
          messageOutput.emit(message);
          emitData = true;
          buffer.clear();
        }
        //String mesg = new String(packet.getData(), packet.getOffset(), packet.getLength());
      } catch (IOException e) {
        throw new RuntimeException("Error reading from channel", e);
      }
      // Even if we miss a readReady because of not locking we will get it again immediately
      readReady = false;
    }
    if (!emitData) {
      synchronized (buffer) {
        try {
          if (!readReady) {
            buffer.wait(inactiveWait);
          }
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @Override
  public void beginWindow(long l)
  {

  }

  @Override
  public void endWindow()
  {

  }

  public final transient DefaultOutputPort<Message> messageOutput = new DefaultOutputPort<Message>();

  @Override
  public void setup(Context.OperatorContext context)
  {
    try {
      //byte[] mesgData = new byte[maxMesgSize];
      //packet = new DatagramPacket(mesgData, maxMesgSize);
      buffer = ByteBuffer.allocate(maxMesgSize);
      action = NetworkManager.getInstance().registerAction(port, NetworkManager.ConnectionType.UDP, this, SelectionKey.OP_READ);
    } catch (IOException e) {
      throw new RuntimeException("Error initializing receiver", e);
    }
  }

  @Override
  public void teardown()
  {
    try {
      NetworkManager.getInstance().unregisterAction(action);
    } catch (Exception e) {
      throw new RuntimeException("Error shutting down receiver", e);
    }
  }

  @Override
  public void ready(NetworkManager.ChannelAction<DatagramChannel> action, int readyOps)
  {
    synchronized (buffer) {
      readReady = true;
      buffer.notify();
    }
  }

  public int getPort()
  {
    return port;
  }

  public void setPort(int port)
  {
    this.port = port;
  }
}
