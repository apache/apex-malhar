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
package org.apache.apex.benchmark.hive;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.SQLException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;
import com.datatorrent.netlet.util.DTThrowable;

public class HiveMapBenchmarkTest
{
  private static final Logger LOG = LoggerFactory.getLogger(HiveMapBenchmarkTest.class);

  @Test
  public void testMethod() throws SQLException
  {
    Configuration conf = new Configuration();
    InputStream inputStream = null;
    try {
      inputStream = new FileInputStream("src/site/conf/dt-site-hive.xml");
    } catch (FileNotFoundException ex) {
      LOG.debug("Exception caught {}", ex);
    }
    conf.addResource(inputStream);
    LOG.debug("conf properties are {}",
        conf.get("dt.application.HiveMapInsertBenchmarkingApp.operator.HiveOperator.store.connectionProperties"));
    LOG.debug("conf dburl is {}",
        conf.get("dt.application.HiveMapInsertBenchmarkingApp.operator.HiveOperator.store.dbUrl"));
    LOG.debug("conf filepath is {}",
        conf.get("dt.application.HiveMapInsertBenchmarkingApp.operator.HiveOperator.store.filepath"));
    LOG.debug("maximum length is {}",
        conf.get("dt.application.HiveMapInsertBenchmarkingApp.operator.RollingFsMapWriter.maxLength"));
    LOG.debug("tablename is {}",
        conf.get("dt.application.HiveMapInsertBenchmarkingApp.operator.HiveOperator.tablename"));
    LOG.debug("permission is {}",
        conf.get("dt.application.HiveMapInsertBenchmarkingApp.operator.RollingFsMapWriter.filePermission"));
    LOG.debug("delimiter is {}",
        conf.get("dt.application.HiveMapInsertBenchmarkingApp.operator.RollingFsMapWriter.delimiter"));

    HiveMapInsertBenchmarkingApp app = new HiveMapInsertBenchmarkingApp();
    LocalMode lm = LocalMode.newInstance();
    try {
      lm.prepareDAG(app, conf);
      LocalMode.Controller lc = lm.getController();
      lc.run(30000);
    } catch (Exception ex) {
      DTThrowable.rethrow(ex);
    }

    IOUtils.closeQuietly(inputStream);
  }

}
