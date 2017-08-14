/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.contrib.zmq;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;


final class ZeroMQMessageReceiver implements Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(ZeroMQMessageReceiver.class);

  public HashMap<String, Integer> dataMap = new HashMap<String, Integer>();
  public int count = 0;
  protected ZMQ.Context context;
  protected ZMQ.Socket subscriber;
  protected ZMQ.Socket syncclient;
  volatile boolean shutDown = false;

  public void setup()
  {
    context = ZMQ.context(1);
    logger.debug("Subsribing on ZeroMQ");
    subscriber = context.socket(ZMQ.SUB);
    subscriber.connect("tcp://localhost:5556");
    subscriber.subscribe("".getBytes());
    syncclient = context.socket(ZMQ.REQ);
    syncclient.connect("tcp://localhost:5557");
    sendSync();
  }

  public void sendSync()
  {
    syncclient.send("".getBytes(), 0);
  }

  @Override
  public void run()
  {
    logger.debug("receiver running");
    while (!Thread.currentThread().isInterrupted() && !shutDown) {
      byte[] msg = subscriber.recv(ZMQ.NOBLOCK);
      // convert to HashMap and save the values for each key
      // then expect c to be 1000, b=20, a=2
      // and do count++ (where count now would be 30)
      if (msg == null || msg.length == 0) {
        continue;
      }
      String str = new String(msg);

      if (str.indexOf("{") == -1) {
        continue;
      }
      int eq = str.indexOf('=');
      String key = str.substring(1, eq);
      int value = Integer.parseInt(str.substring(eq + 1, str.length() - 1));
      logger.debug("\nsubscriber recv: {}", str);
      dataMap.put(key, value);
      count++;
      logger.debug("out of loop.. ");
    }
  }

  public void teardown()
  {
    shutDown = true;
    syncclient.close();
    subscriber.close();
    context.term();
  }
}
