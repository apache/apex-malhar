/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.sink;

import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.netlet.AbstractLengthPrependerClient;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class Server extends com.datatorrent.netlet.Server
{
  private Client client;

  @Override
  public ClientListener getClientConnection(SocketChannel sc, ServerSocketChannel ssc)
  {
    return new Client();
  }

  private class Client extends AbstractLengthPrependerClient
  {
    int idleCount;

    @Override
    public void onMessage(byte[] buffer, int offset, int size)
    {
      assert (size == 12);
      /* first first 8 bytes are the location */
      long location = buffer[offset++]
                      | buffer[offset++] << 8
                      | buffer[offset++] << 16
                      | buffer[offset++] << 24
                      | buffer[offset++] << 32
                      | buffer[offset++] << 40
                      | buffer[offset++] << 48
                      | buffer[offset++] << 56;

      idleCount = buffer[offset++]
                       | buffer[offset++]
                       | buffer[offset++]
                       | buffer[offset++];
    }

    @Override
    public void connected()
    {
      super.connected();
      Server.this.client = this;
    }

    @Override
    public void disconnected()
    {
      Server.this.client = null;
      super.disconnected();
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(Server.class);
}
