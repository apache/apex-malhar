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

import java.util.ArrayList;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import org.apache.apex.malhar.contrib.helper.MessageQueueTestHelper;

class ZeroMQMessageGenerator
{
  private static final Logger logger = LoggerFactory.getLogger(ZeroMQMessageGenerator.class);

  private ZMQ.Context context;
  private ZMQ.Socket publisher;
  private ZMQ.Socket syncservice;
  private final int SUBSCRIBERS_EXPECTED = 1;

  String pubAddr = "tcp://*:5556";
  String syncAddr = "tcp://*:5557";

  public void setup()
  {
    context = ZMQ.context(1);
    logger.debug("Publishing on ZeroMQ");
    publisher = context.socket(ZMQ.PUB);
    publisher.bind(pubAddr);
    syncservice = context.socket(ZMQ.REP);
    syncservice.bind(syncAddr);
  }

  public void send(Object message)
  {
    String msg = message.toString();
    publisher.send(msg.getBytes(), 0);
  }

  public void teardown()
  {
    publisher.close();
    context.term();
  }

  public void generateMessages(int msgCount) throws InterruptedException
  {
    for (int subscribers = 0; subscribers < SUBSCRIBERS_EXPECTED; subscribers++) {
      byte[] value = syncservice.recv(0);
      syncservice.send("".getBytes(), 0);
    }
    for (int i = 0; i < msgCount; i++) {
      ArrayList<HashMap<String, Integer>>  dataMaps = MessageQueueTestHelper.getMessages();
      for (int j = 0; j < dataMaps.size(); j++) {
        send(dataMaps.get(j));
      }
    }
  }
}

