/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.sink;

import com.datatorrent.netlet.AbstractLengthPrependerClient;
import com.datatorrent.netlet.DefaultEventLoop;
import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class Server extends com.datatorrent.netlet.Server
{
  @Override
  public ClientListener getClientConnection(SocketChannel sc, ServerSocketChannel ssc)
  {
    return new AbstractLengthPrependerClient()
    {
      @Override
      public void onMessage(byte[] buffer, int offset, int size)
      {
        logger.debug("Server Received = {}", new String(buffer, offset, size));
        write(DTFlumeSink.MESSAGES.OK.toString().getBytes());
      }

    };
  }

  private static final Logger logger = LoggerFactory.getLogger(Server.class);
}
