package com.datatorrent.demos.udpecho;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;

/**
 * Created by Pramod Immaneni <pramod@datatorrent.com> on 12/11/14.
 */
public class MessageResponder extends BaseOperator
{

  private String responseHeader = "Response: ";

  private int port = 7000;
  private int maxMesgSize = 512;
  private transient NetworkManager.ChannelAction<DatagramChannel> action;
  private transient ByteBuffer buffer;

  public transient final DefaultInputPort<String> messageInput = new DefaultInputPort<String>()
  {
    @Override
    public void process(String message)
    {
      message = responseHeader + message;
      buffer.put(message.getBytes());
      buffer.flip();
      try {
        action.channel.send(buffer, null);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
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
