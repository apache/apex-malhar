package com.datatorrent.demos.udpecho;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;

/**
 * Created by Pramod Immaneni <pramod@datatorrent.com> on 12/11/14.
 */
public class MessageResponder extends BaseOperator
{

  private String responseHeader = "Response: ";

  private int port = 9000;
  private int maxMesgSize = 512;
  private transient NetworkManager.ChannelAction<DatagramSocket> action;
  private transient ByteBuffer buffer;

  public transient final DefaultInputPort<Message> messageInput = new DefaultInputPort<Message>()
  {
    @Override
    public void process(Message message)
    {
      String sendMesg = responseHeader + message.message;
      SocketAddress address = message.socketAddress;
      buffer.put(sendMesg.getBytes());
      buffer.flip();
      try {
        action.channelConfiguration.socket.getChannel().send(buffer, address);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      buffer.clear();
    }
  };

  @Override
  public void setup(Context.OperatorContext context)
  {
    try {
      buffer = ByteBuffer.allocate(maxMesgSize);
      action = NetworkManager.getInstance().registerAction(port, NetworkManager.ConnectionType.UDP, null, 0);
    } catch (IOException e) {
      throw new RuntimeException("Error initializer responder", e);
    }
  }

  @Override
  public void teardown()
  {
    try {
      NetworkManager.getInstance().unregisterAction(action);
    } catch (Exception e) {
      throw new RuntimeException("Error shutting down responder", e);
    }
  }
}
