/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
