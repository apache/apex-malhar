/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.io;

import org.junit.Test;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class WebSocketServerInputOperatorTest
{
  @Test
  public void simpleTest()
  {
    final int port = 6666;

    WebSocketServerInputOperator wssio = new WebSocketServerInputOperator();
    wssio.setPort(port);

    wssio.setup(null);

    try {
      Thread.sleep(3000000L);
    }
    catch(InterruptedException ex) {
      Logger.getLogger(WebSocketServerInputOperatorTest.class.getName()).log(Level.SEVERE, null, ex);
    }
  }
}
