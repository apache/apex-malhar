/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.zmq;

import com.datatorrent.lib.io.SimpleSinglePortInputOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.annotation.ShipContainingJars;

import org.zeromq.ZMQ;

/**
 * <p>Abstract SimpleSinglePortZeroMQPullInputOperator class.</p>
 *
 * @since 0.3.2
 */
@ShipContainingJars(classes={ZMQ.class})
public abstract class SimpleSinglePortZeroMQPullInputOperator<T> extends SimpleSinglePortInputOperator<T> implements Runnable
{
  private transient ZMQ.Context context;
  private transient ZMQ.Socket sock;
  private String zmqAddress = "tcp://127.0.0.1:5555";

  @SuppressWarnings("unused")
  private SimpleSinglePortZeroMQPullInputOperator()
  {
    super();
  }

  public SimpleSinglePortZeroMQPullInputOperator(String addr)
  {
    super();
    zmqAddress = addr;
  }

  @Override
  public void run()
  {
    while (true) {
      byte[] buf = sock.recv(0);
      if (buf == null) {
        continue;
      }
      outputPort.emit(convertFromBytesToTuple(buf));
    }
  }

  protected abstract T convertFromBytesToTuple(byte[] bytes);

  @Override
  public void setup(Context.OperatorContext ctx)
  {
    context = ZMQ.context(1);
    sock = context.socket(ZMQ.PULL);
    sock.connect(zmqAddress);
  }

  @Override
  public void teardown()
  {
    sock.close();
    context.term();
  }

}
