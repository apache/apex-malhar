package com.datatorrent.demos.udpecho;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

/**
 * Created by Pramod Immaneni <pramod@datatorrent.com> on 12/11/14.
 */
public class MessageReceiver implements InputOperator, NetworkManager.ChannelListener<DatagramChannel>
{

  private transient NetworkManager.ChannelAction<DatagramChannel> action;
  private transient ByteBuffer buffer;

  private int port = 7000;
  private int maxMesgSize = 512;
  private int inactiveWait = 10;
  private boolean readReady = false;

  @Override
  public void emitTuples()
  {
    boolean emitData = false;
    if (readReady) {
      DatagramChannel channel = action.channel;
      try {
        int read = channel.read(buffer);
        if (read > 0) {
          StringBuilder sb = new StringBuilder();
          buffer.flip();
          while (buffer.hasRemaining()) {
            sb.append(buffer.getChar());
          }
          messageOutput.emit(sb.toString());
          emitData = true;
          buffer.clear();
        }
      } catch (IOException e) {
        throw new RuntimeException("Error reading from channel", e);
      }
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

  public final transient DefaultOutputPort<String> messageOutput = new DefaultOutputPort<String>();

  @Override
  public void setup(Context.OperatorContext context)
  {
    try {
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
