package com.datatorrent.demos.udpecho;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.channels.SelectionKey;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

/**
 * Created by Pramod Immaneni <pramod@datatorrent.com> on 12/11/14.
 */
public class MessageReceiver implements InputOperator, NetworkManager.ChannelListener<DatagramSocket>
{

  private transient NetworkManager.ChannelAction<DatagramSocket> action;

  //Need the sender info, using a packet for now instead of the buffer
  //private transient ByteBuffer buffer;
  private transient DatagramPacket packet;

  private int port = 7000;
  private int maxMesgSize = 512;
  private int inactiveWait = 10;
  private boolean readReady = false;

  @Override
  public void emitTuples()
  {
    boolean emitData = false;
    if (readReady) {
      DatagramSocket socket = action.channelConfiguration.socket;
      try {
        socket.receive(packet);
        /*
        DatagramChannel channel = action.channel;
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
        }*/
        String mesg = new String(packet.getData(), packet.getOffset(), packet.getLength());
        Message message = new Message();
        message.message = mesg;
        message.socketAddress = packet.getSocketAddress();
        messageOutput.emit(message);
      } catch (IOException e) {
        throw new RuntimeException("Error reading from channel", e);
      }
      // Even if we miss a readReady because of not locking we will get it again immediately
      readReady = false;
    }
    if (!emitData) {
      synchronized (packet) {
        try {
          if (!readReady) {
            packet.wait(inactiveWait);
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
      byte[] mesgData = new byte[maxMesgSize];
      packet = new DatagramPacket(mesgData, maxMesgSize);
      //buffer = ByteBuffer.allocate(maxMesgSize);
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
  public void ready(NetworkManager.ChannelAction<DatagramSocket> action, int readyOps)
  {
    synchronized (packet) {
      readReady = true;
      packet.notify();
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
