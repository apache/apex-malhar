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
package org.apache.apex.malhar.contrib.apachelog;


import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

/**
 * Functional tests for {@link org.apache.apex.malhar.contrib.apachelog.ApacheLogInputGenerator}.
 */
public class ApacheLogInputGeneratorTest
{

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void testInputGenerator() throws InterruptedException
  {
    ApacheLogInputGenerator oper = new ApacheLogInputGenerator();
    CollectorTestSink sink = new CollectorTestSink();
    oper.output.setSink(sink);
    oper.setNumberOfTuples(10);
    oper.setMaxDelay(0);
    oper.setIpAddressFile("src/test/resources/com/datatorrent/contrib/apachelog/ipaddress.txt");
    oper.setUrlFile("src/test/resources/com/datatorrent/contrib/apachelog/urls.txt");
    oper.setAgentFile("src/test/resources/com/datatorrent/contrib/apachelog/agents.txt");
    oper.setRefererFile("src/test/resources/com/datatorrent/contrib/apachelog/referers.txt");
    oper.setup(null);
    oper.activate(null);
    Thread.sleep(100);
    oper.beginWindow(0);
    oper.emitTuples();
    oper.endWindow();
    oper.deactivate();
    oper.teardown();
    Assert.assertEquals(oper.getNumberOfTuples(), sink.collectedTuples.size());
    for (Object collectedTuple : sink.collectedTuples) {
      LOG.debug((String)collectedTuple);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(ApacheLogInputGeneratorTest.class);
}
