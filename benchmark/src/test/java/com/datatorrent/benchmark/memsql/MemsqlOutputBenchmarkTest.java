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

package com.datatorrent.benchmark.memsql;

import com.datatorrent.api.LocalMode;
import java.io.InputStream;
import java.util.logging.Level;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemsqlOutputBenchmarkTest
{
  private static final Logger LOG = LoggerFactory.getLogger(MemsqlOutputBenchmarkTest.class);

  @Test
  public void testMethod() {
    Configuration conf = new Configuration();
    InputStream inputStream = getClass().getResourceAsStream("/dt-site-memsql.xml");
    conf.addResource(inputStream);

    MemsqlOutputBenchmark app = new MemsqlOutputBenchmark();
    LocalMode lm = LocalMode.newInstance();

    try {
      lm.prepareDAG(app, conf);
      LocalMode.Controller lc = lm.getController();
      lc.run(20000);
    }
    catch (Exception ex) {
      java.util.logging.Logger.getLogger(MemsqlInputBenchmarkTest.class.getName()).log(Level.SEVERE, null, ex);
    }

    IOUtils.closeQuietly(inputStream);
  }
}
