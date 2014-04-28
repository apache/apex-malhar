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

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @param <T>
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public abstract class WebSocketServerOperatorBase<T> extends BaseOperator implements WebSocketCallBackBase
{
  EmbeddedWebSocketServer<String, OperatorContext> server;

  @InputPortFieldAnnotation(name = "in", optional = true)
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T t)
    {
      processTuple(t);
    }
  };

  protected abstract void processTuple(T t);

  @Override
  public void setup(OperatorContext context)
  {
    try {
      server = new EmbeddedWebSocketServer<String, OperatorContext>();
      server.setup(context);
      server.setCallBackBase(this);
      server.start();
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void teardown()
  {
    try {
      server.stop();
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
