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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.util.BaseOperator;

/**
 * This is the base implementation of a ZeroMQ output adapter.&nbsp;
 * This operator will behave like a publisher that replies to requests.&nbsp;
 * A concrete operator should be created from this skeleton implementation.
 * <p>
 * <br>
 * Ports:<br>
 * <b>Input</b>: Can have any number of input ports<br>
 * <b>Output</b>: no output port<br>
 * <br>
 * Properties:<br>
 * <b>url</b>:the url for the publisher to listen to subscriber <br>
 * <b>syncUrl</b>: the url for the publisher to synchronize with subscriber<br>
 * <b>SUBSCRIBERS_EXPECTED</b>: the expected number of subscribers<br>
 * <br>
 * Compile time checks:<br>
 * None<br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for AbstractBaseZeroMQOutputOperator&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td>One tuple per key per window per port</td><td><b>400 thousand K,V pairs/s</td><td>Out-bound rate is the main determinant of performance. Operator can process about 400 thousand unique (k,v immutable pairs) tuples/sec as ZeroMQ DAG. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may differ</td></tr>
 * </table><br>
 * </p>
 * @displayName Abstract Base ZeroMQ Input
 * @category Messaging
 * @tags output operator
 * @since 0.3.2
 */
public abstract class AbstractBaseZeroMQOutputOperator extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractBaseZeroMQInputOperator.class);
  protected transient ZMQ.Context context;
  protected transient ZMQ.Socket publisher;
  protected transient ZMQ.Socket syncservice;
  protected int SUBSCRIBERS_EXPECTED = 1;
  private String url;
  private String syncUrl;
  protected boolean syncStarted = false;

  public void setUrl(String url)
  {
    this.url = url;
  }

  public void setSyncUrl(String syncUrl)
  {
    this.syncUrl = syncUrl;
  }

  public void setSUBSCRIBERS_EXPECTED(int expected)
  {
    SUBSCRIBERS_EXPECTED = expected;
  }

  public int getSUBSCRIBERS_EXPECTED()
  {
    return SUBSCRIBERS_EXPECTED;
  }

  @Override
  public void setup(OperatorContext ctx)
  {
    logger.debug("O/P setup");
    context = ZMQ.context(1);
    publisher = context.socket(ZMQ.PUB);
    publisher.bind(url);
    syncservice = context.socket(ZMQ.REP);
    syncservice.bind(syncUrl);
  }

/**
 * necessary for publisher side to synchronize publisher and subscriber, must run after setup()
 * make sure subscribers all connected to the publisher, then the publisher send data after that
 */
  public void startSyncJob()
  {
    for (int subscribers = 0; subscribers < SUBSCRIBERS_EXPECTED; subscribers++) {
      syncservice.recv(0);
      syncservice.send("".getBytes(), 0);
    }
    syncStarted = true;
  }

  @Override
  public void teardown()
  {
    publisher.close();
    context.term();
  }
}
