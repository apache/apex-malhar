package com.datatorrent.benchmark;

import com.datatorrent.api.LocalMode;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.junit.Test;

public class CouchBaseBenchmarkTest
{
  Logger logger = Logger.getLogger("CouchBaseBenchmarkTest.class");

  @Test
  public void testCouchBaseAppOutput() throws Exception
  {
    Configuration conf = new Configuration();
    InputStream is = getClass().getResourceAsStream("/dt-site-couchbase.xml");
    conf.addResource(is);

    conf.get("dt.application.CouchBaseAppOutput.operator.couchbaseOutput.store.uriString");
    conf.get("dt.application.CouchBaseAppOutput.operator.couchbaseOutput.store.password");
    conf.get("dt.application.CouchBaseAppOutput.operator.couchbaseOutput.store.bucket");
    conf.get("dt.application.couchbaseAppOutput.operator.couchbaseOutput.store.max_tuples");
    conf.get("dt.application.couchbaseAppOutput.operator.couchbaseOutput.store.batch_size");
    conf.get("dt.application.couchbaseAppOutput.operator.couchbaseOutput.store.blocktime");
    conf.get("dt.application.couchbaseAppOutput.operator.couchbaseOutput.store.timeout");
    LocalMode lm = LocalMode.newInstance();

    try {
      lm.prepareDAG(new CouchBaseAppOutput(), conf);
      LocalMode.Controller lc = lm.getController();
      //lc.setHeartbeatMonitoringEnabled(false);
      lc.run(20000);
    }
    catch (Exception ex) {
      logger.info(ex.getCause());
    }
    is.close();
  }

  @Test
  public void testCouchBaseAppInput() throws Exception
  {
    Configuration conf = new Configuration();
    InputStream is = getClass().getResourceAsStream("/dt-site-couchbase.xml");
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
    }
    catch (Exception ex) {
      logger.info(ex.getCause());
    }
    is.close();
  }

  @Test
  public void testCouchBaseAppUpdate() throws Exception
  {
    Configuration conf = new Configuration();
    InputStream is = getClass().getResourceAsStream("/dt-site-couchbase.xml");
    conf.addResource(is);
    conf.get("dt.application.CouchBaseAppUpdate.operator.couchbaseUpdate.store.uriString");
    conf.get("dt.application.CouchBaseAppUpdate.operator.couchbaseUpdate.store.bucket");
    conf.get("dt.application.CouchBaseAppUpdate.operator.couchbaseUpdate.store.password");
    conf.get("dt.application.CouchBaseAppUpdate.operator.couchbaseUpdate.store.max_tuples");
    conf.get("dt.application.CouchBaseAppUpdate.operator.couchbaseUpdate.store.batch_size");
    conf.get("dt.application.CouchBaseAppUpdate.operator.couchbaseUpdate.store.blocktime");
    conf.get("dt.application.CouchBaseAppUpdate.operator.couchbaseUpdate.store.timeout");
    LocalMode lm = LocalMode.newInstance();

    try {
      lm.prepareDAG(new CouchBaseAppInput(), conf);
      LocalMode.Controller lc = lm.getController();
      lc.run(20000);
    }
    catch (Exception ex) {
      logger.info(ex.getCause());
    }
    is.close();
  }

}
