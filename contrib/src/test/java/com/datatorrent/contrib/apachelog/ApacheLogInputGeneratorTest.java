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
package com.datatorrent.contrib.apachelog;

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 * Functional tests for {@link com.datatorrent.contrib.apachelog.ApacheLogInputGenerator}.
 */

public class ApacheLogInputGeneratorTest
{

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testInputGenerator()
  {
    ApacheLogInputGenerator oper = new ApacheLogInputGenerator();
    CollectorTestSink sink = new CollectorTestSink();
    oper.output.setSink(sink);
    oper.setNumberOfTuples(10);
    oper.setIpAddressFile("src/test/resources/com/datatorrent/contrib/apachelog/ipaddress.txt");
    oper.setUrlFile("src/test/resources/com/datatorrent/contrib/apachelog/urls.txt");
    oper.setAgentFile("src/test/resources/com/datatorrent/contrib/apachelog/agents.txt");
    oper.setStatusFile("src/test/resources/com/datatorrent/contrib/apachelog/status.txt");
    oper.setup(null);
    oper.beginWindow(0);
    oper.emitTuples();
    oper.endWindow();
    oper.teardown();
    Assert.assertEquals(oper.getNumberOfTuples(), sink.collectedTuples.size());
    for (int i = 0; i < sink.collectedTuples.size(); i++) {
      log.debug((String) sink.collectedTuples.get(i));
    }
  }

  private static Logger log = LoggerFactory.getLogger(ApacheLogInputGeneratorTest.class);
}
