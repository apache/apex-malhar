/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.etl;

import java.util.Map;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class WebSocketOutputOperator<K, V> extends WebSocketServerOperatorBase<Map<K, V>>
{
  @Override
  protected void processTuple(Map<K, V> t)
  {
    String out = "stream out";
    server.process(out);
  }

  @Override
  public String onQuery(String request)
  {
    return "response = " + request;
  }

  @Override
  public String onOpen()
  {
    String schema = "schema";
    return schema;
  }

  @Override
  public void onClose()
  {
  }
}
