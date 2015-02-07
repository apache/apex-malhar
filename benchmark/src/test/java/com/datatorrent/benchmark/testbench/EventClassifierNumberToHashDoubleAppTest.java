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
package com.datatorrent.benchmark.testbench;

import com.datatorrent.api.LocalMode;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Benchmark Test for EventClassifierNumberToHashDouble Operator in local mode.
 */
public class EventClassifierNumberToHashDoubleAppTest
{
  @Test
  public void testEventClassifierNumberToHashDoubleApp() throws FileNotFoundException, IOException
  {
    Logger logger = LoggerFactory.getLogger(EventClassifierNumberToHashDoubleAppTest.class);
    LocalMode lm = LocalMode.newInstance();
    Configuration conf = new Configuration();
    InputStream is = new FileInputStream("src/site/conf/dt-site-testbench.xml");
    conf.addResource(is);
    conf.get("dt.application.EventClassifierNumberToHashDoubleApp.operator.eventClassify.key_keys");
    conf.get("dt.application.EventClassifierNumberToHashDoubleApp.operator.eventClassify.s_start");
    conf.get("dt.application.EventClassifierNumberToHashDoubleApp.operator.eventClassify.s_end");
    try {
      lm.prepareDAG(new EventClassifierNumberToHashDoubleApp(), conf);
      LocalMode.Controller lc = lm.getController();
      lc.run(20000);
    }
    catch (Exception ex) {
       logger.info(ex.getMessage());
    }
    is.close();
  }
}
