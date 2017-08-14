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
package org.apache.apex.malhar.lib.io;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

/**
 * Unit test for emit tuples.
 */
public class ApacheRandomLogsTest
{
  @SuppressWarnings({ "rawtypes", "unchecked", "deprecation" })
  @Test
  public void test()
  {
    ApacheGenRandomLogs oper = new ApacheGenRandomLogs();
    CollectorTestSink sink = new CollectorTestSink();
    oper.outport.setSink(sink);
    oper.setup(null);

    Thread t = new EmitTuples(oper);
    t.start();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      //Fixme
    }
    t.stop();
    Assert.assertTrue("Tuples emitted", sink.collectedTuples.size() > 0);
  }

  private class EmitTuples extends Thread
  {
    private ApacheGenRandomLogs oper;

    public EmitTuples(ApacheGenRandomLogs oper)
    {
      this.oper = oper;
    }

    @Override
    public void run()
    {
      oper.emitTuples();
    }
  }
}
