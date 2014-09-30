package com.datatorrent.benchmark;

import com.datatorrent.api.LocalMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.junit.Test;

/**
 *
 * @author prerna
 */
public class CouchBaseBenchmarkTest {
  Logger logger = Logger.getLogger("CouchBaseBenchmarkTest.class");

    @Test
    public void testCouchBaseAppOutput() throws Exception {
      Configuration conf = new Configuration();
      conf.addResource(getClass().getResourceAsStream("/dt-site-couchbase.xml"));

      logger.info(conf.get("dt.application.CouchBaseAppOutput.operator.couchbaseOutput.store.uriString"));
      logger.info(conf.get("dt.application.CouchBaseAppOutput.operator.couchbaseOutput.store.bucket"));
      logger.info(conf.get("dt.application.CouchBaseAppOutput.operator.couchbaseOutput.store.password"));
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
    }

    /*@Test
    public void testCouchBaseAppInput() throws Exception {
        LocalMode.runApp(new CouchBaseAppInput(), 30000);
    }

    @Test
    public void testCouchBaseAppUpdate() throws Exception {
        LocalMode.runApp(new CouchBaseAppUpdate(), 30000);
    }*/
}
