/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.etl;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public interface WebSocketCallBackBase
{
  public String onOpen();

  public void onClose();

  public String onQuery(String request);
}
