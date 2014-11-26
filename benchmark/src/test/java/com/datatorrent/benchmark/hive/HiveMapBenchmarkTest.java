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
package com.datatorrent.benchmark.hive;

import java.io.InputStream;
import java.sql.SQLException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.contrib.hive.AbstractHiveOutputOperatorTest;
import com.datatorrent.contrib.hive.HiveStore;

import com.datatorrent.api.LocalMode;

import com.datatorrent.benchmark.HiveMapInsertBenchmarkingApp;
import com.datatorrent.common.util.DTThrowable;

public class HiveMapBenchmarkTest
{
  @Test
  public void testMethod() throws SQLException
  {
    Logger LOG = LoggerFactory.getLogger(HiveMapBenchmarkTest.class);
    Configuration conf = new Configuration();
    InputStream inputStream = getClass().getResourceAsStream("/dt-site-hive.xml");
    conf.addResource(inputStream);

    HiveStore store = new HiveStore();
    store.setDbUrl(conf.get("dt.application.HiveMapInsertBenchmarkingApp.operator.HiveMapInsertOperator.store.dbUrl"));
    store.setConnectionProperties(conf.get("dt.application.HiveMapInsertBenchmarkingApp.operator.HiveMapInsertOperator.store.connectionProperties"));
    store.setFilepath(conf.get("dt.application.HiveMapInsertBenchmarkingApp.operator.HiveMapInsertOperator.store.filepath"));
    LOG.info("conf properties are" + conf.get("dt.application.HiveMapInsertBenchmarkingApp.operator.HiveMapInsertOperator.store.connectionProperties"));
    LOG.info("conf dburl is" + conf.get("dt.application.HiveMapInsertBenchmarkingApp.operator.HiveMapInsertOperator.store.dbUrl"));
    LOG.info("conf filepath is" + conf.get("dt.application.HiveMapInsertBenchmarkingApp.operator.HiveMapInsertOperator.store.filepath"));
    LOG.info("conf filename is " + conf.get("dt.application.HiveMapInsertBenchmarkingApp.operator.HiveMapInsertOperator.filename"));
    LOG.info("delimiterMap is " + conf.get("dt.application.HiveMapInsertBenchmarkingApp.operator.HiveMapInsertOperator.delimiter"));
    LOG.info("permission is " + conf.get("dt.application.HiveMapInsertBenchmarkingApp.operator.HiveMapInsertOperator.fsPermission"));
    LOG.info("maximum length is" + conf.get("dt.application.HiveMapInsertBenchmarkingApp.operator.HiveMapInsertOperator.maxLength"));
    HiveMapInsertBenchmarkingApp app = new HiveMapInsertBenchmarkingApp();
    AbstractHiveOutputOperatorTest.hiveInitializeDatabase(store);
    LocalMode lm = LocalMode.newInstance();
    try {
      lm.prepareDAG(app, conf);
      LocalMode.Controller lc = lm.getController();
      lc.run(30000);
    }
    catch (Exception ex) {
      ex.getCause().printStackTrace();
      DTThrowable.rethrow(ex);
    }

    IOUtils.closeQuietly(inputStream);
  }



}
