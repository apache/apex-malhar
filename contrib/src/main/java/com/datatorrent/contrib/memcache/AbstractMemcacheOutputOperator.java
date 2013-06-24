/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
 * limitations under the License. See accompanying LICENSE file.
 */
package com.datatorrent.contrib.memcache;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Memcache output adapter operator, which produce data to Memcached.<p><br>
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: Can have any number of input ports<br>
 * <b>Output</b>: no output port<br>
 * <br>
 * Properties:<br>
 * <b>servers</b>:the memcache server url list<br>
 * <b>client</b>:MemcachedClient instance<br>
 * <br>
 * Compile time checks:<br>
 * None<br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for AbstractMemcacheOutputOperator&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td>One tuple per key per window per port</td><td><b>39 thousand K,V pairs/s</td><td>Out-bound rate is the main determinant of performance. Operator can process about 39 thousand unique (k,v immutable pairs) tuples/sec as Memcache DAG. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may differ</td></tr>
 * </table><br>
 * <br>
 *
 */
public class AbstractMemcacheOutputOperator extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractMemcacheOutputOperator.class);
  protected transient MemcachedClient client;
  private ArrayList<String> servers = new ArrayList<String>();

  @Override
  public void setup(OperatorContext context)
  {
    try {
      client = new MemcachedClient(AddrUtil.getAddresses(servers));
    }
    catch (IOException ex) {
      logger.info(ex.toString());
    }

  }

  public void addServer(String server)
  {
    servers.add(server);
  }

  public void watiForQueue(int timeout) {
    client.waitForQueues(timeout, TimeUnit.SECONDS);
  }

  @Override
  public void teardown()
  {
    client.shutdown(2000, TimeUnit.MILLISECONDS);
  }

}
