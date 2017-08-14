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
package org.apache.apex.benchmark.testbench;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;

/**
 * Benchmark Test for EventIncrementerApp Operator in local mode.
 */
public class EventIncrementerAppTest
{
  @Test
  public void testEventIncrementerApp() throws FileNotFoundException, IOException
  {
    Logger logger = LoggerFactory.getLogger(EventIncrementerAppTest.class);
    LocalMode lm = LocalMode.newInstance();
    Configuration conf = new Configuration();
    InputStream is = new FileInputStream("src/site/conf/dt-site-testbench.xml");
    conf.addResource(is);
    conf.get("dt.application.EventIncrementerApp.operator.hmapOper.seed");
    conf.get("dt.application.EventIncrementerApp.operator.hmapOper.keys");
    conf.get("dt.application.EventIncrementerApp.operator.hmapOper.numKeys");
    try {
      lm.prepareDAG(new EventIncrementerApp(), conf);
      LocalMode.Controller lc = lm.getController();
      lc.run(20000);
    } catch (Exception ex) {
      logger.info(ex.getMessage());
    }
    is.close();
  }
}
