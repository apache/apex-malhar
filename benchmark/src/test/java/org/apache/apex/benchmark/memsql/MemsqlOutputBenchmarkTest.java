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
package org.apache.apex.benchmark.memsql;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.SQLException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.contrib.memsql.AbstractMemsqlOutputOperatorTest;
import org.apache.apex.malhar.contrib.memsql.MemsqlStore;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;
import com.datatorrent.netlet.util.DTThrowable;

public class MemsqlOutputBenchmarkTest
{
  private static final Logger LOG = LoggerFactory.getLogger(MemsqlOutputBenchmarkTest.class);

  @Test
  public void testMethod() throws SQLException, FileNotFoundException
  {
    Configuration conf = new Configuration();
    InputStream inputStream = new FileInputStream("src/site/conf/dt-site-memsql.xml");
    conf.addResource(inputStream);

    MemsqlStore memsqlStore = new MemsqlStore();
    memsqlStore.setDatabaseUrl(conf.get("dt.rootDbUrl"));
    memsqlStore.setConnectionProperties(
        conf.get("dt.application.MemsqlOutputBenchmark.operator.memsqlOutputOperator.store.connectionProperties"));

    AbstractMemsqlOutputOperatorTest.memsqlInitializeDatabase(memsqlStore);

    MemsqlOutputBenchmark app = new MemsqlOutputBenchmark();
    LocalMode lm = LocalMode.newInstance();

    try {
      lm.prepareDAG(app, conf);
      LocalMode.Controller lc = lm.getController();
      lc.run(20000);
    } catch (Exception ex) {
      DTThrowable.rethrow(ex);
    }

    IOUtils.closeQuietly(inputStream);
  }
}
