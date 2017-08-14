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
package org.apache.apex.benchmark;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.datatorrent.api.LocalMode;

public class CouchBaseBenchmarkTest
{
  Logger logger = Logger.getLogger("CouchBaseBenchmarkTest.class");

  @Test
  public void testCouchBaseAppOutput() throws FileNotFoundException, IOException
  {
    Configuration conf = new Configuration();
    InputStream is = new FileInputStream("src/site/conf/dt-site-couchbase.xml");
    conf.addResource(is);

    conf.get("dt.application.CouchBaseAppOutput.operator.couchbaseOutput.store.uriString");
    conf.get("dt.application.CouchBaseAppOutput.operator.couchbaseOutput.store.password");
    conf.get("dt.application.CouchBaseAppOutput.operator.couchbaseOutput.store.bucket");
    conf.get("dt.application.couchbaseAppOutput.operator.couchbaseOutput.store.max_tuples");
    conf.get("dt.application.couchbaseAppOutput.operator.couchbaseOutput.store.queueSize");
    conf.get("dt.application.couchbaseAppOutput.operator.couchbaseOutput.store.blocktime");
    conf.get("dt.application.couchbaseAppOutput.operator.couchbaseOutput.store.timeout");
    LocalMode lm = LocalMode.newInstance();

    try {
      lm.prepareDAG(new CouchBaseAppOutput(), conf);
      LocalMode.Controller lc = lm.getController();
      //lc.setHeartbeatMonitoringEnabled(false);
      lc.run(20000);
    } catch (Exception ex) {
      logger.info(ex.getCause());
    }
    is.close();
  }

  @Test
  public void testCouchBaseAppInput() throws FileNotFoundException, IOException
  {
    Configuration conf = new Configuration();
    InputStream is = new FileInputStream("src/site/conf/dt-site-couchbase.xml");
    conf.addResource(is);
    conf.get("dt.application.CouchBaseAppInput.operator.couchbaseInput.store.uriString");
    conf.get("dt.application.CouchBaseAppInput.operator.couchbaseInput.store.blocktime");
    conf.get("dt.application.CouchBaseAppInput.operator.couchbaseInput.store.timeout");
    conf.get("dt.application.CouchBaseAppInput.operator.couchbaseInput.store.bucket");
    conf.get("dt.application.CouchBaseAppInput.operator.couchbaseInput.store.password");
    LocalMode lm = LocalMode.newInstance();

    try {
      lm.prepareDAG(new CouchBaseAppInput(), conf);
      LocalMode.Controller lc = lm.getController();
      lc.run(20000);
    } catch (Exception ex) {
      logger.info(ex.getCause());
    }
    is.close();
  }

}
