package com.datatorrent.demos.oldfaithful;

import com.datatorrent.api.LocalMode;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class OldFaithfulApplicationTest
{
  @Test
  public void testSomeMethod() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    OldFaithfulApplication app = new OldFaithfulApplication();
    app.populateDAG(lma.getDAG(), new Configuration(false));

    try {
      LocalMode.Controller lc = lma.getController();
      lc.setHeartbeatMonitoringEnabled(false);
      lc.run(50000);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }
}
