package com.datatorrent.benchmark;

import com.datatorrent.api.LocalMode;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author prerna
 */
public class CouchBaseBenchmarkTest
{
 Logger LOG = LoggerFactory.getLogger(CouchBaseBenchmarkTest.class);
 Configuration conf = new Configuration();
/*
  @Test
  public void testCouchBaseAppOutput() throws Exception
  {
    conf.addResource(getClass().getResourceAsStream("/dt-site-couchbase.xml"));
    LOG.debug("==============={}",conf.get("dt.application.CouchBaseAppOutput.operator.couchbaseOutput.store.uriString"));
    LOG.debug("================={}",conf.get("dt.application.CouchBaseAppOutput.operator.couchbaseOutput.store.bucket"));
    LOG.debug("================={}",conf.get("dt.application.CouchBaseAppOutput.operator.couchbaseOutput.store.password"));
    LocalMode.runApp(new CouchBaseAppOutput(),conf, 30000);

  }
*/
  @Test
  public void testCouchBaseAppInput() throws Exception
  {
    Configuration conf = new Configuration();
    conf.addResource(getClass().getResourceAsStream("/dt-site-couchbase.xml"));
    LOG.debug("==============={}",conf.get("dt.application.CouchBaseAppInput.operator.couchbaseInput.store.uriString"));
    LOG.debug("================={}",conf.get("dt.application.CouchBaseAppInput.operator.couchbaseInput.store.bucket"));
    LOG.debug("================={}",conf.get("dt.application.CouchBaseAppInput.operator.couchbaseInput.store.password"));
    LocalMode.runApp(new CouchBaseAppInput(),conf, 30000);
  }
  /*
  @Test
  public void testCouchBaseAppUpdate() throws Exception
  {
    LocalMode.runApp(new CouchBaseAppUpdate(), 30000);
  }*/

}
